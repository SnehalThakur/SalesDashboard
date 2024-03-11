"""
Copyright (c) 2024 - Bizware International
"""

from fastapi import APIRouter, Depends, status, Response, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from com.bizware import app

import com.bizware.schemas as schemas
import com.bizware.models as models
# import src.oauth2 as oauth2
from com.bizware.services.Pymongodb import get_session
from com.bizware.utils.utils import verify


router = APIRouter(
    prefix = "/api/auth",
    tags=['Authentication']
)

@router.post('/login', response_model=schemas.Token)
async def login(user_credentials: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):

    user = db.query(models.User).filter(models.User.email == user_credentials.username).first()

    if not user:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Credentials")

    if not verify(user_credentials.password, user.password):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Invalid Credentials')

    access_token = oauth2.create_access_token(data = {"user_id" : user.id})

    return {
            "access_token" : access_token,
            "token_type" : "bearer"
        }

@router.get('/logout', response_model=schemas.Token)
async def logout(request: Request, response_model=HTMLResponse):
    auth_token  = request.cookies.get('Authorization')

    if (auth_token):
        redirect = RedirectResponse(app.ui_router.url_path_for('signin'))
        redirect.set_cookie('Authorization', '')
        return redirect

    return RedirectResponse(app.ui_router.url_path_for('home'))