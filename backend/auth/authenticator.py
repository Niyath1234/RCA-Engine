"""
JWT Authenticator

JWT-based authentication implementation.
"""

import time
from typing import Optional, Dict, Any
from datetime import datetime, timedelta


class JWTAuthenticator:
    """JWT-based authenticator."""
    
    def __init__(self, secret_key: str, algorithm: str = 'HS256', 
                 token_expiry_hours: int = 24):
        """
        Initialize JWT authenticator.
        
        Args:
            secret_key: Secret key for signing tokens
            algorithm: JWT algorithm (default: HS256)
            token_expiry_hours: Token expiry in hours
        """
        try:
            import jwt
        except ImportError:
            raise ImportError("PyJWT is required. Install with: pip install PyJWT")
        
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.token_expiry_hours = token_expiry_hours
        self.jwt = jwt
    
    def authenticate(self, token: str) -> Dict[str, Any]:
        """
        Authenticate user token.
        
        Args:
            token: JWT token string
        
        Returns:
            Authentication result dictionary
        """
        try:
            # Decode and verify token
            payload = self.jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm]
            )
            
            # Check expiration
            exp = payload.get('exp')
            if exp and time.time() > exp:
                return {
                    'success': False,
                    'error': 'Token expired'
                }
            
            return {
                'success': True,
                'user_id': payload.get('user_id'),
                'email': payload.get('email'),
                'roles': payload.get('roles', []),
                'permissions': payload.get('permissions', []),
            }
            
        except self.jwt.ExpiredSignatureError:
            return {
                'success': False,
                'error': 'Token expired'
            }
        except self.jwt.InvalidTokenError as e:
            return {
                'success': False,
                'error': f'Invalid token: {str(e)}'
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'Authentication failed: {str(e)}'
            }
    
    def generate_token(self, user_id: str, email: Optional[str] = None,
                      roles: Optional[list] = None,
                      permissions: Optional[list] = None) -> str:
        """
        Generate authentication token.
        
        Args:
            user_id: User ID
            email: User email
            roles: List of user roles
            permissions: List of user permissions
        
        Returns:
            JWT token string
        """
        payload = {
            'user_id': user_id,
            'email': email,
            'roles': roles or [],
            'permissions': permissions or [],
            'iat': datetime.utcnow(),
            'exp': datetime.utcnow() + timedelta(hours=self.token_expiry_hours),
        }
        
        token = self.jwt.encode(
            payload,
            self.secret_key,
            algorithm=self.algorithm
        )
        
        return token
    
    def validate_permission(self, user_permissions: list, required_permission: str) -> bool:
        """
        Validate user has required permission.
        
        Args:
            user_permissions: List of user permissions
            required_permission: Required permission
        
        Returns:
            True if user has permission, False otherwise
        """
        return required_permission in user_permissions or 'admin' in user_permissions

