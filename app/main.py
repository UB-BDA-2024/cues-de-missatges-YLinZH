import fastapi
import yoyo
from yoyo import step

from .sensors.controller import router as sensorsRouter

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

#TODO: Apply new TS migrations using Yoyo
#Read docs: https://ollycope.com/software/yoyo/latest/
# Configuració de la connexió a la base de dades Timescale
ts_db_url = "postgresql://timescale:timescale@timescale:5433/timescale"

# Crea una instància de Yoyo per a les migracions Timescale
ts_migrations = yoyo.read_migrations('migrations_ts')
ts_backend = yoyo.get_backend(ts_db_url)

# Funció per aplicar les migracions Timescale
def apply_ts_migrations():
    
    with ts_backend.lock():
            ts_backend.apply_migrations(ts_migrations)
            ts_backend.rollback_migrations(ts_backend.to_rollback(ts_migrations))

# Event handler per aplicar les migracions Timescale en l'inici de l'aplicació
@app.on_event("startup")
def startup_event():
    apply_ts_migrations()


app.include_router(sensorsRouter)

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
