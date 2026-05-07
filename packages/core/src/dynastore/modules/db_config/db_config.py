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

import os
import dynastore.tools.class_tools as class_tools

class DBConfig:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://testuser:testpassword@db:5432/gis_dev"
    )
    pool_min_size: int = int(os.getenv("DB_POOL_MIN_SIZE", "5"))
    pool_max_size: int = int(os.getenv("DB_POOL_MAX_SIZE", "100"))
    pool_max_queries: int = int(os.getenv("DB_POOL_MAX_QUERIES", "50000"))
    pool_max_inactive_connection_lifetime: int = int(os.getenv("DB_POOL_MAX_INACTIVE_CONNECTION_LIFETIME", "30"))
    pool_command_timeout: int = int(os.getenv("DB_POOL_COMMAND_TIMEOUT", "60"))
    connect_timeout: int = int(os.getenv("DB_CONNECT_TIMEOUT", "30"))
    
    def __repr__(self) -> str:
        return class_tools.__repr__(self, sensitive_attrs=["database_url"])
