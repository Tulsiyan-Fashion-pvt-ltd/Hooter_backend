class AuthorizationError(Exception):
    """Raised when a user is unauthorized to access a resource."""


class ShopifyAPIError(Exception):
    """Raised when Shopify API returns errors or invalid responses."""


class ValidationError(Exception):
    """Raised when input validation fails."""


class IdempotencyConflict(Exception):
    """Raised when an idempotency key conflict is detected."""