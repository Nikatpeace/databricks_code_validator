"""YAML-based configuration management for validation rules."""

import yaml
import os
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from pathlib import Path
import sys

# Set up logging (to be configured via config.py integration)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class RuleConfig:
    """Configuration for a single validation rule."""
    enabled: bool = True
    description: str = ""
    severity: str = "error"  # error, warning, info
    type: str = "regex"  # regex, llm, custom (new field for rule dispatch)
    parameters: Dict[str, Any] = field(default_factory=dict)

    def validate(self):
        """Validate rule configuration."""
        valid_severities = ["error", "warning", "info"]
        if self.severity not in valid_severities:
            raise ValueError(f"Invalid severity '{self.severity}'. Must be one of {valid_severities}")


@dataclass
class ValidatorConfig:
    """Configuration for a validator category."""
    enabled: bool = True
    rules: Dict[str, RuleConfig] = field(default_factory=dict)


@dataclass
class ValidationConfig:
    """Complete validation configuration."""
    global_config: Dict[str, Any] = field(default_factory=dict)
    code_quality_validation: ValidatorConfig = field(default_factory=ValidatorConfig)
    performance_validation: ValidatorConfig = field(default_factory=ValidatorConfig)
    governance_validation: ValidatorConfig = field(default_factory=ValidatorConfig)
    security_validation: ValidatorConfig = field(default_factory=ValidatorConfig)
    custom_validation: ValidatorConfig = field(default_factory=ValidatorConfig)
    output: Dict[str, Any] = field(default_factory=dict)
    reporting: Dict[str, Any] = field(default_factory=dict)


class YamlConfigManager:
    """Manages YAML-based configuration for validation rules."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the YAML configuration file.
                        If None, uses default config or env var override.
        """
        self.config_path = config_path or os.getenv('CONFIG_PATH')
        self.config: Optional[ValidationConfig] = None
        
    def load_config(self, config_path: Optional[str] = None) -> ValidationConfig:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to the YAML configuration file
            
        Returns:
            ValidationConfig object
        """
        if config_path:
            self.config_path = config_path
            
        if not self.config_path:
            # Use default config file with env var override support
            default_config_path = Path(__file__).parent.parent.parent.parent / "config" / "validation_rules.yaml"
            self.config_path = str(default_config_path)
        
        if not os.path.exists(self.config_path):
            logger.error(f"Configuration file not found: {self.config_path}")
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                yaml_data = yaml.safe_load(file)
            if not yaml_data:
                raise ValueError("Empty or invalid YAML configuration")
            self.config = self._parse_yaml_config(yaml_data)
            return self.config
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML configuration: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading config: {e}")
            raise
    
    def _parse_yaml_config(self, yaml_data: Dict[str, Any]) -> ValidationConfig:
        """Parse YAML data into ValidationConfig object."""
        config = ValidationConfig()
        
        # Global configuration
        config.global_config = yaml_data.get('global', {})
        
        # Parse validator configurations
        validator_sections = [
            'code_quality_validation',
            'performance_validation',
            'governance_validation',
            'security_validation',
            'custom_validation'
        ]
        
        for section in validator_sections:
            if section in yaml_data:
                validator_config = self._parse_validator_config(yaml_data[section])
                setattr(config, section, validator_config)
        
        # Output and reporting configuration
        config.output = yaml_data.get('output', {})
        config.reporting = yaml_data.get('reporting', {})
        
        return config
    
    def _parse_validator_config(self, validator_data: Dict[str, Any]) -> ValidatorConfig:
        """Parse validator section from YAML."""
        validator_config = ValidatorConfig()
        validator_config.enabled = validator_data.get('enabled', True)
        
        rules_data = validator_data.get('rules', {})
        for rule_name, rule_data in rules_data.items():
            rule_config = RuleConfig(
                enabled=rule_data.get('enabled', True),
                description=rule_data.get('description', ''),
                severity=rule_data.get('severity', 'error'),
                type=rule_data.get('type', 'regex'),
                parameters=rule_data.get('parameters', {})
            )
            rule_config.validate()  # Validate rule config
            validator_config.rules[rule_name] = rule_config
        
        return validator_config
    
    def get_config(self) -> ValidationConfig:
        """Get the current configuration."""
        if self.config is None:
            self.config = self.load_config()
        return self.config
    
    def is_rule_enabled(self, validator_name: str, rule_name: str) -> bool:
        """
        Check if a specific rule is enabled.
        
        Args:
            validator_name: Name of the validator (e.g., 'code_quality_validation')
            rule_name: Name of the rule (e.g., 'default_language_match')
            
        Returns:
            True if the rule is enabled
        """
        config = self.get_config()
        
        validator_config = getattr(config, validator_name, None)
        if not validator_config or not validator_config.enabled:
            return False
        
        rule_config = validator_config.rules.get(rule_name)
        if not rule_config:
            return False
        
        return rule_config.enabled
    
    def get_rule_config(self, validator_name: str, rule_name: str) -> Optional[RuleConfig]:
        """
        Get configuration for a specific rule.
        
        Args:
            validator_name: Name of the validator
            rule_name: Name of the rule
            
        Returns:
            RuleConfig object or None if not found
        """
        config = self.get_config()
        
        validator_config = getattr(config, validator_name, None)
        if not validator_config:
            return None
        
        return validator_config.rules.get(rule_name)
    
    def get_rule_parameters(self, validator_name: str, rule_name: str) -> Dict[str, Any]:
        """
        Get parameters for a specific rule.
        
        Args:
            validator_name: Name of the validator
            rule_name: Name of the rule
            
        Returns:
            Dictionary of rule parameters
        """
        rule_config = self.get_rule_config(validator_name, rule_name)
        if rule_config:
            return rule_config.parameters
        return {}
    
    def save_config(self, output_path: str):
        """
        Save current configuration to a YAML file.
        
        Args:
            output_path: Path to save the configuration file
        """
        if not self.config:
            logger.error("No configuration loaded to save")
            raise ValueError("No configuration loaded to save")
        
        # Convert config back to dictionary format for YAML serialization
        config_dict = self._config_to_dict(self.config)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as file:
                yaml.dump(config_dict, file, default_flow_style=False, indent=2)
        except Exception as e:
            logger.error(f"Failed to save config to {output_path}: {e}")
            raise
    
    def _config_to_dict(self, config: ValidationConfig) -> Dict[str, Any]:
        """Convert ValidationConfig back to dictionary format."""
        result = {}
        
        if config.global_config:
            result['global'] = config.global_config
        
        # Convert validator configurations
        validators = {
            'code_quality_validation': config.code_quality_validation,
            'performance_validation': config.performance_validation,
            'governance_validation': config.governance_validation,
            'security_validation': config.security_validation,
            'custom_validation': config.custom_validation
        }
        
        for name, validator in validators.items():
            if validator.rules:  # Only include if there are rules
                result[name] = {
                    'enabled': validator.enabled,
                    'rules': {}
                }
                
                for rule_name, rule_config in validator.rules.items():
                    result[name]['rules'][rule_name] = {
                        'enabled': rule_config.enabled,
                        'description': rule_config.description,
                        'severity': rule_config.severity,
                        'type': rule_config.type,
                        'parameters': rule_config.parameters
                    }
        
        if config.output:
            result['output'] = config.output
        
        if config.reporting:
            result['reporting'] = config.reporting
        
        return result


def create_default_config() -> ValidationConfig:
    """Create a default validation configuration."""
    config = ValidationConfig()
    
    # Set default global configuration
    config.global_config = {
        'enabled': True,
        'batch_size': 5,
        'max_retries': 3,
        'timeout_seconds': 30
    }
    
    # Add default rules for each validator
    config.code_quality_validation = ValidatorConfig(
        enabled=True,
        rules={
            'default_language_match': RuleConfig(
                enabled=True,
                description="Notebook default language should match majority of code cells",
                severity="error"
            ),
            'valid_languages_only': RuleConfig(
                enabled=True,
                description="Only approved languages should be used",
                severity="error",
                parameters={'allowed_languages': ['sql', 'python']}
            )
        }
    )
    
    config.performance_validation = ValidatorConfig(
        enabled=True,
        rules={
            'avoid_select_star': RuleConfig(
                enabled=True,
                description="Avoid using SELECT * in queries to reduce data scanning",
                severity="error",
                parameters={'disallow_select_star': True}
            )
        }
    )
    
    config.governance_validation = ValidatorConfig(
        enabled=True,
        rules={
            'unity_catalog_usage': RuleConfig(
                enabled=True,
                description="Use Unity Catalog for tables and views",
                severity="error",
                parameters={'require_three_level_namespace': True}
            )
        }
    )
    
    config.security_validation = ValidatorConfig(
        enabled=True,
        rules={
            'no_hard_coded_secrets': RuleConfig(
                enabled=True,
                description="Avoid hard-coding secrets like tokens or passwords",
                severity="error",
                parameters={'suggested_alternative': 'dbutils.secrets.get()'}
            )
        }
    )
    
    return config