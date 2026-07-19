class AuthorizationError(Exception):
    """Raised when a user is unauthorized to access a resource."""
    def __init__(self):
        super().__init__("Unauthorized access to the requested resource.")
        self.error_code = 455

    def __str__(self):
        return f"{self.args[0]} (Error Code: {self.error_code})"


class ShopifyAPIError(Exception):
    """Raised when Shopify API returns errors or invalid responses."""
    def __init__(self):
        super().__init__("Shopify API returned an error or invalid response.")
        self.error_code = 510

    def __str__(self):
        return f"{self.args[0]} (Error Code: {self.error_code})"


class ValidationError(Exception):
    """Raised when input validation fails."""
    def __init__(self):
        super().__init__("Input validation failed.")
        self.error_code = 412

    def __str__(self):
        return f"{self.args[0]} (Error Code: {self.error_code})"


class IdempotencyConflict(Exception):
    """Raised when an idempotency key conflict is detected."""
    def __init__(self):
        super().__init__("Idempotency key conflict.")
        self.error_code = 525

    def __str__(self):
        return f"{self.args[0]} (Error Code: {self.error_code})"