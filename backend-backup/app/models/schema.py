from pydantic import BaseModel
from typing import Dict, Any

class SignalingMessage(BaseModel):
    type: str
    data: Dict[str, Any]
    
    