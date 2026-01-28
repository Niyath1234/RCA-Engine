"""
Configuration Manager

Centralized configuration management with environment variable support.
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigManager:
    """Centralized configuration manager."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to config file (defaults to config/config.yaml)
        """
        self.config_file = config_file or self._get_default_config_path()
        self.config = self._load_config()
        self._validate_config()
    
    def _get_default_config_path(self) -> str:
        """Get default config file path."""
        project_root = Path(__file__).parent.parent.parent
        return str(project_root / 'config' / 'config.yaml')
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file and environment."""
        config = {}
        
        # Load from file if exists
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f) or {}
        else:
            # Use defaults
            config = self._get_default_config()
        
        # Override with environment variables
        config = self._override_with_env(config)
        
        return config
    
    def _override_with_env(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Override config values with environment variables."""
        # Database config
        if os.getenv('DB_HOST'):
            config.setdefault('databases', {}).setdefault('postgres', {})['host'] = os.getenv('DB_HOST')
        if os.getenv('DB_PORT'):
            config.setdefault('databases', {}).setdefault('postgres', {})['port'] = int(os.getenv('DB_PORT'))
        if os.getenv('DB_NAME'):
            config.setdefault('databases', {}).setdefault('postgres', {})['database'] = os.getenv('DB_NAME')
        if os.getenv('DB_USER'):
            config.setdefault('databases', {}).setdefault('postgres', {})['user'] = os.getenv('DB_USER')
        if os.getenv('DB_PASSWORD'):
            config.setdefault('databases', {}).setdefault('postgres', {})['password'] = os.getenv('DB_PASSWORD')
        
        # LLM config
        if os.getenv('OPENAI_API_KEY'):
            config.setdefault('llm', {})['api_key'] = os.getenv('OPENAI_API_KEY')
        if os.getenv('LLM_MODEL'):
            config.setdefault('llm', {})['model'] = os.getenv('LLM_MODEL')
        if os.getenv('LLM_TEMPERATURE'):
            config.setdefault('llm', {})['temperature'] = float(os.getenv('LLM_TEMPERATURE'))
        
        # Redis config
        if os.getenv('REDIS_HOST'):
            config.setdefault('cache', {}).setdefault('redis', {})['host'] = os.getenv('REDIS_HOST')
        if os.getenv('REDIS_PORT'):
            config.setdefault('cache', {}).setdefault('redis', {})['port'] = int(os.getenv('REDIS_PORT'))
        
        return config
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            'databases': {
                'postgres': {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'rca_engine',
                    'user': 'postgres',
                    'password': '',
                    'pool_size': 10,
                    'timeout': 30,
                }
            },
            'llm': {
                'provider': 'openai',
                'model': 'gpt-4',
                'api_key': '',
                'temperature': 0.0,  # Deterministic
                'max_tokens': 3000,
                'timeout': 120,
            },
            'cache': {
                'enabled': True,
                'type': 'memory',  # or 'redis'
                'ttl': 3600,
                'max_size': 1000,
                'redis': {
                    'host': 'localhost',
                    'port': 6379,
                    'db': 0,
                }
            },
            'security': {
                'rate_limit': {
                    'enabled': True,
                    'requests_per_minute': 60,
                    'requests_per_hour': 1000,
                },
                'sql_injection_protection': True,
                'query_timeout': 30,
                'max_rows': 10000,
                'default_limit': 1000,
            },
            'invariants': {
                'enforce_determinism': True,
                'enforce_boundary': True,
                'enforce_reproducibility': True,
                'enforce_fail_closed': True,
            },
            'execution': {
                'sandbox': {
                    'max_execution_time': 30,
                    'max_rows': 10000,
                    'default_limit': 1000,
                    'read_only_role': 'rca_readonly',
                    'enforce_ordering': True,
                    'enforce_limit': True,
                },
                'firewall': {
                    'enabled': True,
                    'blocked_patterns': [],
                },
                'kill_switch': {
                    'enabled': True,
                }
            },
            'observability': {
                'logging': {
                    'level': 'INFO',
                    'format': 'json',
                    'file': 'logs/rca_engine.log',
                },
                'metrics': {
                    'enabled': True,
                    'export_interval': 60,
                }
            },
            'deployment': {
                'feature_flags': {
                    'multi_step_planning': False,
                    'query_optimization': False,
                    'advanced_caching': False,
                    'rag_versioning': False,
                },
                'shadow_mode': {
                    'enabled': False,
                }
            }
        }
    
    def _validate_config(self):
        """Validate configuration."""
        # Check required fields
        required_fields = [
            'databases',
            'llm',
            'security',
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Required config field missing: {field}")
    
    def get_database_config(self, db_name: str = 'postgres') -> Dict[str, Any]:
        """Get database configuration."""
        return self.config.get('databases', {}).get(db_name, {})
    
    def get_llm_config(self) -> Dict[str, Any]:
        """Get LLM configuration."""
        return self.config.get('llm', {})
    
    def get_cache_config(self) -> Dict[str, Any]:
        """Get cache configuration."""
        return self.config.get('cache', {})
    
    def get_security_config(self) -> Dict[str, Any]:
        """Get security configuration."""
        return self.config.get('security', {})
    
    def get_invariants_config(self) -> Dict[str, Any]:
        """Get invariants configuration."""
        return self.config.get('invariants', {})
    
    def get_execution_config(self) -> Dict[str, Any]:
        """Get execution configuration."""
        return self.config.get('execution', {})
    
    def get_observability_config(self) -> Dict[str, Any]:
        """Get observability configuration."""
        return self.config.get('observability', {})
    
    def get_deployment_config(self) -> Dict[str, Any]:
        """Get deployment configuration."""
        return self.config.get('deployment', {})
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key path (e.g., 'llm.temperature')."""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value if value is not None else default


# Global config instance
_config_instance: Optional[ConfigManager] = None


def get_config(config_file: Optional[str] = None) -> ConfigManager:
    """Get global config instance."""
    global _config_instance
    if _config_instance is None:
        _config_instance = ConfigManager(config_file)
    return _config_instance

