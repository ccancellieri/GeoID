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

"""
Serverless job execution protocol definitions.
"""

from typing import Protocol, Optional, Dict, List, Any, runtime_checkable

@runtime_checkable
class JobExecutionProtocol(Protocol):
    """
    Protocol for serverless job execution operations, enabling decoupled access
    to job management without direct dependency on specific cloud providers.
    
    This protocol is designed to be optional - implementations should handle
    unavailability gracefully. Can be implemented by Cloud Run, AWS Lambda,
    Azure Functions, or any other serverless execution platform.
    """
    
    async def run_job(
        self,
        job_name: str,
        args: Optional[List[str]] = None,
        env_vars: Optional[Dict[str, str]] = None
    ) -> Any:
        """
        Triggers a serverless job asynchronously.
        
        Args:
            job_name: The name of the job to execute
            args: Optional list of command-line arguments to pass to the job's container
            env_vars: Optional dictionary of environment variables to override
            
        Returns:
            Operation object for the job execution
            
        Raises:
            RuntimeError: If the job cannot be triggered
        """
        ...
    
    async def get_job_config(self) -> Dict[str, str]:
        """
        Discovers deployed jobs and returns a mapping of task_type to job_name.
        
        Returns:
            Dictionary mapping task_type identifiers to job names
        """
        ...

