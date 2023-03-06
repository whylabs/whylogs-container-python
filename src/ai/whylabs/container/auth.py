from fastapi import Body, Depends, FastAPI, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer

from .config import ContainerConfig

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")  # use token authentication


class Auth:
    def __init__(self) -> None:
        self.env_vars = ContainerConfig()

    def api_key_auth(self, api_key: str = Depends(oauth2_scheme)) -> None:
        password = self.env_vars.container_password
        if password is None:
            return

        if not api_key == password:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Forbidden")
