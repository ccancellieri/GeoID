#    Copyright 2025 FAO
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
# 
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""titiler Templating Extensions."""
import time
import json
from typing import Optional, List, Tuple, cast
from attrs import define
from jinja2 import Template, Environment
from pydantic import BaseModel, HttpUrl, types
from itertools import zip_longest
from fastapi import Query, HTTPException, Header
from fastapi.responses import StreamingResponse, Response
import ast
import asyncio
from aiohttp.client_exceptions import ContentTypeError
from typing_extensions import Annotated
from dynastore.tools.pydantic import FlexibleDictParam, TemplateParam
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Query, status, Request, Body, Depends, APIRouter
from typing import Optional, Dict, Any, AsyncGenerator, Union
import xml.sax
import xml.sax.handler
from xml.sax.handler import ContentHandler
import httpx
from pydantic import AnyUrl, confloat, HttpUrl
from contextlib import asynccontextmanager
# LooseHeaders / LooseCookies from aiohttp are incompatible with httpx; define as plain dict aliases
LooseHeaders = Dict[str, Any]
LooseCookies = Dict[str, Any]

import xml.etree.ElementTree as ET
import dynastore.extensions.httpx.httpx_service as httpx_service
from pydantic import AnyUrl, confloat
from dynastore.tools.cache import cached
from dynastore.extensions import ExtensionProtocol, get_extension_instance

try:
    from lxml import etree as lxml_etree  # type: ignore[import-not-found]
except ImportError:
    lxml_etree = None
try:
    import ijson  # type: ignore[import-not-found]
except ImportError:
    ijson = None
try:
    import xmltodict
except ImportError:
    xmltodict = None
    

import logging

logger = logging.getLogger(__name__)


jinja_env = Environment()
# {"enable_async":True}

def _extract_headers(h_list: Any = None, headers: Any = None) -> dict:
    _headers = {}
    if isinstance(h_list, list):
        for h in h_list:
            h = h.lower()
            if h not in headers:
                raise Exception(f"Headers {h} not found in headers, unable to send request for h_list: {h_list}")
            _headers[h]=headers.get(h)

    elif isinstance(h_list, dict):
        for k, v in h_list.items():
            k = k.lower()
            if k not in headers:
                raise Exception(f"Headers {k} not found in headers, unable to send request for h_list: {h_list}")
            _headers[v] = headers.get(k)

    return _headers


async def _prepare_template_and_model(client: httpx.AsyncClient, model: Optional[dict], m_urls: Optional[dict], m_urls_h: Optional[dict], template: Optional[str], t_url: Optional[str] = None, t_url_h: Any = None, mergeModel: Optional[bool] = False) -> Tuple[Optional[str], dict]:
    
    if m_urls:
        # parallel fetch
        # let's download also the template

        url_list=[]
        key_list=[]
        as_json=[]
        h_list=[]
        if t_url:
            url_list=[t_url]
            as_json=[False]
            h_list=[t_url_h]
            key_list=[]

        for key, model_url in m_urls.items():
            url_list.append(model_url)
            as_json.append(True)
            h_list.append(m_urls_h.get(key) if m_urls_h else None)
            key_list.append(key)

        resolved_list = await download_all(client=client, urls=url_list, as_json=as_json, h_list=h_list)
        
        if t_url:
            template = resolved_list[0]
            m_urls.update(dict(zip(key_list, resolved_list[1:])))
        else:
            m_urls.update(dict(zip(key_list, resolved_list)))
    else:
        if t_url:
            assert template == None
            # single fetch
            template = await download_file(client, t_url, False, t_url_h)
        else:
            assert template != None

    interpolation_models = {}
    if mergeModel:
        if model:
            interpolation_models.update(model)
        if m_urls:
            for key in m_urls.keys():
                val = m_urls.get(key)
                if val is not None:
                    interpolation_models.update(val)
    else:
        interpolation_models = {
            "m" : model or {},
            "m_urls": m_urls or {}
        }
    return template, interpolation_models


async def _interpolate_streaming(template, model: dict, escapeTemplate: bool = False, autoEscape: bool = False) -> AsyncGenerator[bytes, None]:
    
    # Apply escaping if necessary
    if escapeTemplate:
        template = bytes(template, 'utf-8').decode('unicode_escape')

    # Prepare the Jinja2 template
    # Assuming templates are loaded in-memory
    # TODO load from params other options
    jinja_template = jinja_env.from_string(template)
    # Apply auto-escaping if necessary

    jinja_template.autoescape = autoEscape  # type: ignore[reportAttributeAccessIssue]
    # jinja_template = Template(source=template, autoescape=autoEscape)
    try:
        for chunk in jinja_template.stream(model):
            yield chunk.encode("utf-8")
    except Exception as e:
        yield f"Interpolation error: {str(e)}".encode("utf-8")

def _interpolate(template: str, model: dict, escapeTemplate: bool = False, autoEscape: bool = False) -> str:
    
    # Apply escaping if necessary
    if escapeTemplate:
        template = bytes(template, 'utf-8').decode('unicode_escape')

    # Prepare the Jinja2 template
    # Assuming templates are loaded in-memory
    # TODO load from params other options
    jinja_template = jinja_env.from_string(template)

    # Apply auto-escaping if necessary
    jinja_template.autoescape = autoEscape  # type: ignore[reportAttributeAccessIssue]
    # jinja_template = Template(source=template, autoescape=autoEscape)

    return jinja_template.render(model)
        
async def _resolve(client: httpx.AsyncClient, template_str: str, resolve_urls: list, resolve_urls_h: list[dict] = []) -> str:
    
    #with stream as s:
    # template_obj=''.join(chunk for chunk in stream)
    try:
        interpolated_template = json.loads(template_str)
    except Exception as e:
        raise Exception(
            f"Unable to resolve {resolve_urls} the template is not a regular json. Exception: {str(e)}. Template: {template_str}")

    urls=[]
    as_json = []
    for key in resolve_urls:
        if key in interpolated_template:
            url = interpolated_template[key]
            try:
                HttpUrl(url)
            except Exception:
                raise Exception(f"Url to be resolved for key: {key} is not valid, invalid url: {url}")
            urls.append(url)
            as_json.append(True)
        else:
            raise Exception(f"Parameter {key} is not in the document")

    interpolated_template.update(dict(zip(resolve_urls, await download_all(client, urls, as_json, cast(Optional[List[LooseHeaders]], resolve_urls_h)))))
    try:
        return json.dumps(interpolated_template)
    except Exception as e:
        raise Exception(f"Error serialising resolved template: {e}") from e

async def _fetch(client: httpx.AsyncClient, url: str, as_json: bool = False, headers: Optional[LooseHeaders] = None, cookies: Optional[LooseCookies] = None, timeout: int = 3, sleep_time: float = 0.01, max_retry_count: int = 1):
    """
    Fetches data from the given URL with retry logic and robust error handling.
    Args:
        session: An aiohttp.ClientSession object.
        url: The URL to fetch data from.
        as_json: If True, attempts to parse the response as JSON. Defaults to False.
        headers: Optional headers for the request.
        cookies: Optional cookies for the request.
        timeout: The timeout for the request in seconds. Defaults to 3 seconds.
        sleep_time: The time to sleep between retries in seconds. Defaults to 0.01 seconds.
        max_retry_count: The maximum number of retries. Defaults to 1.

    Returns:
        The fetched data as either JSON or text, depending on the `as_json` argument.
        Returns None on failure after all retries.
    """
    headers = headers or {}
    cookies = cookies or {}

    for attempt in range(max_retry_count + 1):
        try:
            logger.info(f"Downloading {url} (Attempt {attempt+1}/{max_retry_count+1})")
            response = await client.get(url, headers=headers, timeout=timeout, cookies=cookies)
            response.raise_for_status()
            if as_json:
                try:
                    return response.json()
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decoding error for {url}: {str(e)}")
                    return response.text
            else:
                return response.text

        except (httpx.HTTPError, json.JSONDecodeError, Exception) as e:
            if isinstance(e, httpx.HTTPError):
                error_type = "httpx error"
                log_level = logger.warning
            elif isinstance(e, json.JSONDecodeError):
                error_type = "JSON error"
                log_level = logger.warning
            else:
                error_type = "Unexpected error"
                log_level = logger.error
            
            log_level(f"{error_type} fetching {url} (Attempt {attempt+1}/{max_retry_count+1}): {str(e)}")

            if attempt < max_retry_count:
                await asyncio.sleep(sleep_time)
            else:
                logger.error(f"Failed to fetch {url} after {max_retry_count+1} attempts: {str(e)}")
                raise e

    raise Exception(f"Unable to fetch {url}")

async def download_all(client: httpx.AsyncClient, urls: List[str], as_json: List[bool], h_list: Optional[List[LooseHeaders]] = None, c_list: Optional[List[LooseCookies]] = None):
    """Downloads multiple URLs, handling varying header and cookie list lengths."""

    if not urls:
        return []

    if not as_json:
        as_json = [True] * len(urls) #default to json if not provided
    
    assert len(as_json) == len(urls), "The length of as_json must match the length of urls."

    _h_list: List[LooseHeaders] = list(h_list) if h_list is not None else [{}] * len(urls)
    _c_list: List[LooseCookies] = list(c_list) if c_list is not None else [{}] * len(urls)

    # Ensure header and cookie lists are at least as long as the URL list
    _h_list = _h_list[:len(urls)] + [{}] * (len(urls) - len(_h_list))
    _c_list = _c_list[:len(urls)] + [{}] * (len(urls) - len(_c_list))
    
    tasks = [
        _fetch(client=client, url=url, as_json=json_flag, headers=headers, cookies=cookies)
        for url, json_flag, headers, cookies in zip(urls, as_json, _h_list, _c_list)
    ]
    return await asyncio.gather(*tasks)

async def download_file(client: httpx.AsyncClient, url: str, as_json: bool = True, headers: Optional[LooseHeaders] = None, cookies: Optional[LooseCookies] = None):
    """Downloads a single file, handling optional headers and cookies."""

    if headers is None:
        headers = {}
    if cookies is None:
        cookies = {}

    return await _fetch(client=client, url=url, as_json=as_json, headers=headers, cookies=cookies)

# Security settings
MAX_XML_SIZE = 1024 * 1024 * 1024  # 1GB (streaming allows larger files)
CHUNK_SIZE = 1024 * 64  # 64KB
HTTP_TIMEOUT = 30.0

class StreamingXMLHandler(xml.sax.ContentHandler):
    
    def __init__(self):
        self.stack = []
        self.current = {}
        self.result = None
        self.attr_prefix = '@'
        self.exclude_attributes = False
        self.size = 0
        
    def startElement(self, name, attrs):
        self.size += len(name) + sum(len(k) + len(v) for k, v in attrs.items())
        if self.size > MAX_XML_SIZE:
            raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "XML size exceeds limit")
            
        new_element = {}
        if not self.exclude_attributes:
            for k in attrs.getNames():
                new_element[f"{self.attr_prefix}{k}"] = attrs.getValue(k)
        
        if self.stack:
            parent = self.stack[-1]
            if name in parent:
                if isinstance(parent[name], list):
                    parent[name].append(new_element)
                else:
                    parent[name] = [parent[name], new_element]
            else:
                parent[name] = new_element
        else:
            self.result = new_element
            
        self.stack.append(new_element)
        
    def endElement(self, name):
        self.stack.pop()
        
    def characters(self, content):
        if content.strip():
            current = self.stack[-1]
            if '#text' in current:
                current['#text'] += content.strip()
            else:
                current['#text'] = content.strip()
class StreamParser:
    def __init__(self):
        self.parser: Any = xml.sax.make_parser()
        self.handler = StreamingXMLHandler()
        self.parser.setContentHandler(self.handler)
        self.parser.setFeature(xml.sax.handler.feature_external_ges, False)
        self.parser.setFeature(xml.sax.handler.feature_external_pes, False)

    async def feed(self, chunk: bytes):
        try:
            self.parser.feed(chunk)
        except Exception as e:
            raise HTTPException(400, f"XML parsing error: {str(e)}")

    def close(self):
        self.parser.close()

@asynccontextmanager
async def stream_xml_source(source: AsyncGenerator[bytes, None]):
    parser = StreamParser()
    try:
        # Process all chunks first
        async for chunk in source:
            await parser.feed(chunk)
        
        # Close parser after processing all data
        parser.close()
        
        # Yield final result once
        yield parser.handler.result
    finally:
        # The parser might already be closed, so we add a check.
        if parser and hasattr(parser, 'close'):
            parser.close()

async def stream_xml_url(client: httpx.AsyncClient, url: httpx.URL) -> AsyncGenerator[bytes, None]:
    try:
        async with client.stream(
            'GET',
            url,
            headers={"Accept": "application/xml"},
            timeout=HTTP_TIMEOUT
        ) as response:
            response.raise_for_status()
            if response.headers.get('Content-Type', '').split(';')[0] not in {"application/xml", "text/xml"}:
                raise HTTPException(status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, "Invalid content type")
            async for chunk in response.aiter_bytes(CHUNK_SIZE):
                yield chunk
    except httpx.HTTPError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Error fetching XML: {str(e)}")



# Security settings
MAX_XML_IN_MEMORY_SIZE = 1024 * 1024 * 5  # 5MB
HTTP_TIMEOUT = 10.0
ALLOWED_CONTENT_TYPES = {"text/xml", "application/xml"}

# Use cached to memoize parser configurations
@cached(maxsize=1, distributed=False)
def get_safe_parser_config():
    return {
        "xmltodict": {
            "attr_prefix": "@",
            "disable_entities": True,
            "process_namespaces": False,
            "namespace_separator": ":"
        },
        "elementtree": {
            "strip_whitespace": True,
            "attr_prefix": "@"
        },
        "lxml": {
            "strip_whitespace": True,
            "resolve_entities": False,
            "attr_prefix": "@"
        }
    }

def elementtree_to_dict(element, config: dict):
    """Secure ElementTree to dict converter"""
    attr_prefix = config['attr_prefix']
    exclude_attributes = config.get('exclude_attributes', False)
    
    result = {}
    
    if not exclude_attributes and element.attrib:
        result.update({f"{attr_prefix}{k}": v for k, v in element.attrib.items()})
    
    if element.text and element.text.strip():
        if len(result) == 0:
            result = element.text.strip()
        else:
            result['#text'] = element.text.strip()
    
    for child in element:
        child_dict = elementtree_to_dict(child, config)
        
        if child.tag in result:
            if isinstance(result[child.tag], list):
                result[child.tag].append(child_dict)
            else:
                result[child.tag] = [result[child.tag], child_dict]
        else:
            result[child.tag] = child_dict
    
    return result

async def fetch_xml(client: httpx.AsyncClient, url: httpx.URL) -> str:
    """Safely fetch XML content from URL with security constraints"""
    try:
        response = await client.get(
            url,
            headers={"User-Agent": "SecureXMLConverter/1.0"},
            timeout=HTTP_TIMEOUT
        )
        response.raise_for_status()
        
        if response.headers.get('Content-Type', '').split(';')[0] not in ALLOWED_CONTENT_TYPES:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail="URL does not return XML content"
            )
            
        if int(response.headers.get('Content-Length', 0)) > MAX_XML_IN_MEMORY_SIZE:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"XML content exceeds maximum size of {MAX_XML_IN_MEMORY_SIZE//1024//1024}MB"
            )
            
        return response.text[:MAX_XML_IN_MEMORY_SIZE]
        
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error fetching XML from URL: {str(e)}"
        )

async def convert_xml(xml_data: str, config: dict, parser: str) -> Dict[str, Any]:
    """Core XML conversion logic with security hardening"""
    try:
        parser_config = get_safe_parser_config()[parser]
        
        if parser == "elementtree":
            root = ET.fromstring(xml_data)
            return elementtree_to_dict(root, parser_config)
            
        elif parser == "lxml":
            if not lxml_etree:
                raise HTTPException(400, "lxml parser requires `pip install lxml`")
            
            safe_parser = lxml_etree.XMLParser(
                resolve_entities=False,
                huge_tree=False,
                strip_cdata=True,
                remove_blank_text=parser_config['strip_whitespace']
            )
            root = lxml_etree.fromstring(xml_data, parser=safe_parser)
            return elementtree_to_dict(root, parser_config)
            
        elif parser == "xmltodict":
            if not xmltodict:
                raise HTTPException(400, "xmltodict parser requires `pip install xmltodict`")
            
            return xmltodict.parse(
                xml_data,
                **parser_config,
                postprocessor=lambda path, key, value: (
                    key.replace(':', '_'), value
                ) if ':' in key else (key, value))
            
        else:
            raise HTTPException(400, "Invalid parser specified")
            
    except Exception as e:
        raise HTTPException(400, f"XML parsing error: {str(e)}")

def _check_headers(which_headers, requests_object, headers, param_name):
    if which_headers:
        try:
            if isinstance(which_headers, str):
                which_headers = json.loads(which_headers)
            assert requests_object, f"Unable to use {param_name} if the matching structure param is undefined"
            if isinstance(requests_object, list):
                requests_keys = requests_object
            elif isinstance(requests_object, dict):
                requests_keys = requests_object.keys()
            elif isinstance(requests_object, str):
                requests_keys = [ requests_object ]
            else:
                raise Exception(f"Unsupported request_object: {requests_object}")
            
            for which_headers_key in which_headers:
                if which_headers_key in requests_keys:
                    which_headers[which_headers_key]=_extract_headers(which_headers[which_headers_key], headers=headers)
                else:
                    raise HTTPException(status_code=500, detail=f"{param_name} keys must match its corresponding object keys (key not found: {which_headers_key})")
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Parameter '{param_name}' should be a valid object: {str(which_headers)}\nException: {str(e)}")
class TemplatingExtension(ExtensionProtocol):
    priority: int = 100

    router:APIRouter = APIRouter(prefix="/template", tags=["Template API"])

    def __init__(self, app: FastAPI):
        logger.info("Template extension: Initializing.")
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols import HttpxProtocol
        self.httpx_extension = get_protocol(HttpxProtocol)
        if not self.httpx_extension:
            logger.warning("Template extension: 'httpx' extension not found. URL fetching capabilities will be disabled.")

    @router.get("/resolve_url", response_description="Resolve the url in a json object")
    async def resolve_url(
        request: Request,  # type: ignore
        url: str = Query(..., description="The json url to download"),
        key_list: list = Query(..., description="The list of keys to resolve"),
    ):
        from dynastore.extensions.httpx.httpx_service import get_httpx_client
        client: httpx.AsyncClient = await get_httpx_client(request)

        file_json = await download_file(client, url, as_json=True)
        if not isinstance(file_json, dict):
            raise HTTPException(status_code=500, detail="Downloaded file is not a JSON object")
        for key in key_list:
            if key in file_json:
                file_json[key] = await download_file(client, file_json[key], as_json=True)
            else:
                logger.warning(f"Key {key} does not exist")

        return file_json

    @router.get("/interpolate", response_description="Render template with urls to files and a model")
    async def interpolate_template_get(
        request: Request,  # type: ignore
        t: Optional[TemplateParam] = Query(default=None, description="The Jinja2 template"),
        t_url: Optional[str] = Query(default=None, description="URL of the Jinja2 template file"),
        t_url_h: Optional[FlexibleDictParam] = Query(default=None,description="The model (dict as JSON or Python-style string)"),
        m_urls: Optional[FlexibleDictParam] = Query(default=None, description="Dict of URLs pointing to JSON model files"),
        m_urls_h: Optional[FlexibleDictParam] = Query(default=None, description="Dict of Headers list names to pass to each m_url the key name of each list may match the m_url keys"),
        m: Optional[FlexibleDictParam]= Query(default=None, description="The model (dict as JSON or Python-style)"),
        ru: Optional[FlexibleDictParam] = Query(default=None, description="The optional list of key to be resolved"),
        ru_h: Optional[FlexibleDictParam] = Query(default=None, description="Dict of Headers list names to pass to each ru the key name of each list may match the ru, can have less elements"),
        
        r_mt: Optional[str] = Query(default=None, description="Returned media_type: text/csv, [application/json], text/html"),
        et: Optional[bool] = Query(default=None, description="Template should be escaped [False]"),
        ae: Optional[bool] = Query(default=None, description="Template should be auto escaped [False]"),
        mm: Optional[bool] = Query(default=None, description="Model should be merged [False]"),
        stream: Optional[bool] = Query(default=None, description="May stream the output. Use false for json objects. [False]")
    ):
        # Import the dependency locally to avoid startup errors if httpx is not present.
        from dynastore.extensions.httpx.httpx_service import get_httpx_client
        client: httpx.AsyncClient = await get_httpx_client(request)

        headers = {}
        for k,v in request.headers.items():
            headers.update({k.lower():v})

        # parameter validations
        if t and t_url:
            raise HTTPException(status_code=500, detail="Please specify only one 't' or 't_url'")
        elif not t and not t_url:
            raise HTTPException(status_code=500, detail="Please specify at least one 't' or 't_url'")
        
        if t and not r_mt:
            if isinstance(t, str):
                r_mt = "text/html"
            elif isinstance(t, dict):
                t = json.dumps(t)
                r_mt = "application/json"
            else:
                raise HTTPException(
                    status_code=500, detail="value of parameter 't' should be a valid string or dict")
        
        if t_url:
            try:
                HttpUrl(t_url)
            except Exception:
                raise HTTPException(
                    status_code=500, detail=f"value of parameter 't_url' should be a valid url: {str(t_url)}")
            
            _check_headers(t_url_h, t_url, headers, "t_url_h")

        if m and isinstance(m,str):
            try:
                m = json.loads(m)
            except Exception:
                raise HTTPException(
                    status_code=500, detail=f"parameter 'm' should be a valid object: {str(m)}")
            
        if ru and isinstance(ru,str):
            try:
                ru = json.loads(ru)
            except Exception:
                try:
                    ru = ast.literal_eval(ru)
                except Exception:
                    raise HTTPException(
                        status_code=500, detail="parameter 'ru' should be a valid list")
            
            _check_headers(ru_h, ru, headers, "ru_h")
            
        if m_urls:
            if isinstance(m_urls, str):
                try:
                    m_urls = json.loads(m_urls)
                except Exception:
                    raise HTTPException(
                        status_code=500, detail="Parameter 'm_urls' should be a valid object")
            
            for url in (m_urls or {}).values():
                try:
                    HttpUrl(url)
                except Exception:
                    raise HTTPException(status_code=500, detail="values of parameter 'm_urls' should be valid urls")
                
            _check_headers(m_urls_h, m_urls, headers, "m_urls_h")

        try:
            template, model = await _prepare_template_and_model(client, m_urls=m_urls, m_urls_h=m_urls_h, model=m, template=t, t_url=t_url, t_url_h=t_url_h, mergeModel=mm)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Exception: {str(e)}")
        
        assert template is not None, "Template must be set at this point"
        _et: bool = et or False
        _ae: bool = ae or False
        try:
            if ru:
                assert not stream, "Stream can't be true when resolve url is provided, it assumes an in memory model"
                if not r_mt:
                    r_mt = "application/json"
                return Response(
                        await _resolve(client,
                            template_str=_interpolate(
                                template=template, model=model, escapeTemplate=_et, autoEscape=_ae),
                            resolve_urls=ru, resolve_urls_h=cast(List[dict], ru_h) if ru_h else []),
                        media_type=r_mt)
            else:
                if stream:
                    return StreamingResponse(_interpolate_streaming(template=template, model=model, escapeTemplate=_et, autoEscape=_ae), media_type=r_mt)
                else:
                    if not r_mt:
                        r_mt = "application/json"
                    return Response(_interpolate(template=template, model=model, escapeTemplate=_et, autoEscape=_ae), media_type=r_mt)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Exception: {str(e)}")
    class InterpolationRequest(BaseModel):
        t : Optional[str]
        t_url: Optional[str]
        t_url_h: Optional[Dict[str, Any]]
        m_urls : Optional[Dict[str, Any]]
        m_urls_h : Optional[Dict[str, Any]]
        m: Optional[Dict[str, Any]]
        r_mt: Optional[str]
        ru: Optional[Dict[str, Any]]
        ru_h: Optional[Dict[str, Any]]
        et: Optional[bool]
        ae: Optional[bool]
        mm: Optional[bool]
        stream: Optional[bool]
        
    @router.post("/interpolate", response_description="Render template with urls to files and a model")
    async def interpolate_template_post(request: InterpolationRequest):  # type: ignore[misc]
        t = request.t
        t_url = request.t_url
        t_url_h = request.t_url_h
        m_urls = request.m_urls
        m_urls_h = request.m_urls_h
        m = request.m
        r_mt = request.r_mt
        ru = request.ru
        ru_h = request.ru_h
        et = request.et
        ae = request.ae
        mm = request.mm
        stream = request.stream

        return await interpolate_template_get(request=request, t=t, t_url=t_url, t_url_h=t_url_h, m=m, m_urls=m_urls, m_urls_h=m_urls_h, ru=ru, ru_h=ru_h, ae=ae, et=et, mm=mm, r_mt=r_mt, stream=stream)  # type: ignore[name-defined]
    
    @router.post("/xml-to-json/stream")
    async def stream_parse_post(
        request: Request,  # type: ignore
        parser: str = Query("sax", enum=["sax"]),
        attr_prefix: str = Query("@"),
        exclude_attributes: bool = Query(False)
    ):
        from dynastore.extensions.httpx.httpx_service import get_httpx_client
        client: httpx.AsyncClient = await get_httpx_client(request)

        """Streaming XML parser endpoint for large files"""
        async def post_stream():
            total_size = 0
            async for chunk in request.stream():
                if total_size + len(chunk) > MAX_XML_SIZE:
                    raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "XML content too large")
                total_size += len(chunk)
                yield chunk
                
        async with stream_xml_source(post_stream()) as result:
            return result

    @router.get("/xml-to-json/stream")
    async def stream_parse_get(
        xml_url: AnyUrl,  # type: ignore
        request: Request,
        parser: str = Query("sax", enum=["sax"]),
        attr_prefix: str = Query("@"),
        exclude_attributes: bool = Query(False)
    ):
        """Streaming XML parser for URL sources"""
        handler = StreamingXMLHandler()
        handler.attr_prefix = attr_prefix
        handler.exclude_attributes = exclude_attributes
        
        try:
            from dynastore.extensions.httpx.httpx_service import get_httpx_client
            client: httpx.AsyncClient = await get_httpx_client(request)

            async with stream_xml_source(stream_xml_url(client, httpx.URL(str(xml_url)))) as result:
                return result
        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e))

    
    @router.post("/xml-to-json/",
            summary="Convert XML to JSON with security hardening",
            response_description="Converted JSON structure")
    async def post_xml_to_json(
        xml_data: str = Body(..., media_type="application/xml", embed=True,  # type: ignore
                            max_length=MAX_XML_SIZE,
                            example="<root><item id='1'>Content</item></root>"),
        parser: str = Query("elementtree", 
                        description="Parser to use: elementtree, lxml, or xmltodict",
                        enum=["elementtree", "lxml", "xmltodict"]),
        pretty_print: bool = Query(False, description="Format JSON output with indentation"),
        exclude_attributes: bool = Query(False, description="Exclude XML attributes"),
    ):
        try:
            """
            Secure XML to JSON conversion with:
            - Entity processing disabled by default
            - Size limits
            - Secure parser configurations
            """
            config = get_safe_parser_config()[parser].copy()
            config["exclude_attributes"] = exclude_attributes
            
            result = await convert_xml(xml_data, config, parser)
            
            return json.loads(
                json.dumps(
                    result,
                    indent=2 if pretty_print else None,
                    default=str
                )
            )
        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e))

    @router.get("/xml-to-json/",
            summary="Convert XML from URL to JSON",
            response_description="Converted JSON structure")
    async def get_xml_to_json(
        request: Request,  # type: ignore
        xml_url: AnyUrl = Query(..., description="URL to fetch XML from"),
        parser: str = Query("elementtree", 
                        description="Parser to use: elementtree, lxml, or xmltodict",
                        enum=["elementtree", "lxml", "xmltodict"]),
        pretty_print: bool = Query(False, description="Format JSON output with indentation"),
        exclude_attributes: bool = Query(False, description="Exclude XML attributes"),
        max_age: Annotated[float, Field(strict=True, ge=1, le=300)] = Query(60.0, 
                                            description="Cache-Control max-age in seconds")
    ):
        """
        Secure URL-based XML conversion with:
        - SSRF protection
        - Content type validation
        - Size limits
        - Timeout constraints
        """
        headers = {
            "Cache-Control": f"public, max-age={max_age}",
            "X-Content-Type-Options": "nosniff"
        }
        
        try:

            from dynastore.extensions.httpx.httpx_service import get_httpx_client
            client: httpx.AsyncClient = await get_httpx_client(request)

            # Convert to HTTPX-compatible URL
            xml_content = await fetch_xml(client, httpx.URL(str(xml_url)))
            config = get_safe_parser_config()[parser].copy()
            config["exclude_attributes"] = exclude_attributes
            
            result = await convert_xml(xml_content, config, parser)
            
            return json.loads(
                json.dumps(
                    result,
                    indent=2 if pretty_print else None,
                    default=str
                )
            )
            
        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e))
# async def main():
    
#     test={
#     "urls" : {},
#     "template" : "https://storage.googleapis.com/interpolation_tests/demo_v2/config.json",
#     "model" : {
#         "project_name": "demo_project",
#         "language": "es",
#         "terriajs_base_url": "https://fao.org",
#         "with_bing": False,
#         "titiler_base_url": "https://localhost:8080/interpolate",
#         "bucket_base_url": "https://storage.googleapis.com/interpolation_tests/demo_v2",
#         "google_analytics_key": "UA-AAAAA"
#     },
#     "resolve_urls" : ['initializationUrls', 'parameters'],
#     "escapeTemplate" : False,
#     "autoEscape" : False
#     }

#     result = await interpolate_and_resolve(**test)
#     print(result)


# if __name__ == "__main__":
    
#     app=FastAPI()

#     @app.get("/interpolate", response_description="Render template with urls to files and a model")
#     async def interpolate_template_get(
#         urls: str = Query(..., description="Dict of URLs pointing to JSON model files"),
#         t: str = Query(..., description="URL of the Jinja2 template file"),
#         m: Optional[str] = Query(description="The model", default="{}"),
#         ru: Optional[str] = Query(description="The optional list of key to be resolved", default="[]"),
#         et: Optional[bool] = Query(description="Template should be escaped", default=False),
#         ae: Optional[bool] = Query(description="Template should be auto escaped", default=False)
#     ):
#         print(interpolate(urls, t, m, ru, et, ae))
#     import uvicorn
#     # uvicorn.run(app, host="0.0.0.0", port=8080)
#     asyncio.run(main())
