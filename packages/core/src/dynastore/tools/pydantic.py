
import json
import ast
import logging
from typing import Any, Optional, Dict, Callable, Annotated, Union
from pydantic.functional_validators import BeforeValidator

def parse_dict(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        parsed = ast.literal_eval(value)
        # if not isinstance(parsed, dict):
        #     raise ValueError("Input must be a dictionary")
        return parsed

# Define an annotated type that applies the parse_dict function
FlexibleDictParam = Annotated[Any, BeforeValidator(parse_dict)]

def parse_template(value: str) -> Union[str, dict]:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(value)
            if not isinstance(parsed, dict):
                raise ValueError("Input must be a dictionary")
            return parsed
        except Exception as e:
            logging.warning(f"Unable to load template as a json, using plain/text")
        return value


TemplateParam = Annotated[Any, BeforeValidator(parse_template)]
