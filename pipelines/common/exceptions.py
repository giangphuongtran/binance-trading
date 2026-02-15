class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""

class ExternalServiceError(Exception):
    """Raised when external service is unavailable."""

    def __init__(self, service: str, message: str, status_code: int | None = None):
        super().__init__(f"{service}: {message}")
        self.service = service
        self.message = message
        self.status_code = status_code

class SchemaValidationError(Exception):
    """Raised when schema validation fails."""
    def __init__(self, schema_name: str, message: str, payload_preview: str | None = None):
        super().__init__(f"{schema_name}: {message}")
        self.schema_name = schema_name
        self.message = message
        self.payload_preview = payload_preview