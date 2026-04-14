from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db_name: str = "rag5"
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    llm_provider: str = "openai"  # "openai" or "anthropic"
    embedding_model: str = "text-embedding-3-small"
    chunk_size: int = 1000
    chunk_overlap: int = 200
    top_k: int = 5
    retriever: str = "vector"  # vector | keyword | hybrid | multi_query | agent
    multi_query_rewrites: int = 3
    agent_max_iterations: int = 3
    debug: bool = False
    testuser: str = "bob@test.com"
    proxy_user_header: str = "x-forwarded-user"

    # Prefect orchestration + upload staging
    upload_storage_dir: str = "/tmp/rag5_uploads"

    model_config = {"env_file": ".env"}


settings = Settings()
