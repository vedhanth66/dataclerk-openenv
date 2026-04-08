from fastapi import FastAPI

def main():
    app = FastAPI()

    @app.get("/")
    def root():
        return {"status": "ok"}

    return app