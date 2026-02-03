from dataclasses import dataclass

# --> Use Better Secret Management Later
@dataclass
class MongoCredential:
    protocol: str = "mongodb"
    # host: str = "localhost"
    host: str = "mongodb"
    port: str = "27017"
    path: str = "/"