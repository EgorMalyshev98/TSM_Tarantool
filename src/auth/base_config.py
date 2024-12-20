from fastapi.security import APIKeyHeader
from fastapi_users import FastAPIUsers
from fastapi_users.authentication import AuthenticationBackend, BearerTransport, JWTStrategy

from src.auth.manager import get_user_manager
from src.auth.models import User
from src.config import DEV, SECRET_AUTH

bearer_transport = BearerTransport("auth")
is_optional_auth = DEV
auth_header = APIKeyHeader(name="Authorization", auto_error=False)


def get_jwt_strategy() -> JWTStrategy:
    return JWTStrategy(secret=SECRET_AUTH, lifetime_seconds=3600)


auth_backend = AuthenticationBackend(
    name="jwt",
    transport=bearer_transport,
    get_strategy=get_jwt_strategy,
)

fastapi_users = FastAPIUsers[User, int](
    get_user_manager,
    [auth_backend],
)


current_user = fastapi_users.current_user(optional=is_optional_auth)
current_verified_user = fastapi_users.current_user(active=True, verified=True, optional=is_optional_auth)
