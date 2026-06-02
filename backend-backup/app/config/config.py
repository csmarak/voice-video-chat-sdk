from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    
    APP_NAME: str = "Voice SDK SFU mvp"
    HOST: str = "0.0.0.0"
    PORT: int = 8080
    
    
    class Config:
        env_file = ".env"
        

settings = Settings()