import secrets

print("JWT_SECRET=" + secrets.token_urlsafe(48))
print("INTERNAL_PROXY_KEY=" + secrets.token_urlsafe(40))
