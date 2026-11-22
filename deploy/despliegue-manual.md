# Despliegue manual de SD Calendario sobre Ray

Cada tipo de nodo se construye y se arranca con su propio `Dockerfile` y un
`docker run`. El despliegue de referencia usa 1 head, 5 workers y 3 APIs sin
estado: 5 réplicas Raft, una por nodo, con quórum 3/5.

## 0. Scripts de apoyo

```bash
./deploy/up.sh          # construye las imágenes y levanta todo el despliegue
./deploy/down.sh        # detiene los contenedores y borra los volúmenes
./deploy/demo.sh        # propuesta, reintento con el mismo msg_id y estado
./deploy/caos-nivel2.sh # caos de nivel 2: 2 APIs + 2 nodos caídos a la vez
./deploy/caos-any-two.sh # prueba de recuperación ante cada par de contenedores
```

Variables: `SDCAL_NETWORK` (red), `SDCAL_IMAGE` (prefijo de imágenes),
`SDCAL_WORKERS` (5), `SDCAL_APIS` (3) y `SDCAL_SKIP_BUILD=1` para reutilizar las
imágenes ya construidas. `SDCAL_API_TOKENS` debe contener un objeto JSON que
asocie cada usuario con uno o varios tokens Bearer de al menos 32 caracteres.
Los tokens se inyectan en las APIs y no se guardan en el repositorio.

## 1. Crear la red y las imágenes

Desde la raíz del repositorio:

```bash
docker network create sdcal
docker build -f deploy/Dockerfile -t sdcal-ray-head .
docker build -f deploy/Dockerfile.worker -t sdcal-ray-worker .
docker build -f deploy/Dockerfile.api -t sdcal-ray-api .
```

La red `sdcal` proporciona resolución DNS entre contenedores. El nombre
`ray-head` debe conservarse porque lo usan los workers y la API.

## 2. Arrancar el head

```bash
docker run -d --restart unless-stopped --name ray-head --network sdcal \
  -p 6379:6379 -p 8265:8265 \
  -v sdcal-gcs-data:/app/data/gcs \
  sdcal-ray-head
```

El entrypoint `deploy/ray-head.sh` arranca Ray con el recurso
`sdcal-node-head`, que identifica el head de forma estable aunque Ray le
asigne otro NodeID tras un reinicio. También desactiva el soporte `atfork` de
gRPC (`GRPC_ENABLE_FORK_SUPPORT=0`) para evitar fallos al iniciar los procesos
del proxy de Ray Client. GCS persiste su metadato en el volumen
`sdcal-gcs-data` para recuperar el clúster al reiniciar el contenedor. Esta
recuperación usa el backend RocksDB experimental de Ray 2.60; no cubre la
pérdida física del host Docker.

Esperar a que el head esté listo antes de arrancar los workers:

```bash
docker logs ray-head
```

## 3. Arrancar los workers

```bash
for index in 1 2 3 4 5; do
  docker run -d --restart unless-stopped \
    --name "ray-worker-$index" --network sdcal \
    -e SDCAL_NODE_ID="$index" \
    -v "sdcal-data-$index:/app/data" \
    sdcal-ray-worker
done
```

Cada worker declara un recurso único `sdcal-node-<id>`. El planificador de
Raft usa ese recurso para fijar una réplica por nodo: el recurso sobrevive al
reinicio del contenedor, a diferencia del NodeID de Ray. El volumen
`sdcal-data-<id>` guarda el SQLite local de la réplica, de modo que el nodo
recupera su log y su snapshot al volver.

Comprobar que los cinco contenedores siguen activos:

```bash
docker ps --filter name=ray-
```

Si un worker cae, el supervisor espera a que vuelva para recrear la réplica
sobre su SQLite local; no la mueve a otro nodo.

## 4. Arrancar las APIs

```bash
for index in 1 2 3; do
  port=$((8080 + index))
  docker run -d --restart unless-stopped \
    --name "sdcal-api-$index" --network sdcal \
    -e SDCAL_API_TOKENS="$SDCAL_API_TOKENS" -p "$port:8080" \
    sdcal-ray-api
done
```

Las tres APIs se conectan al Ray Client del head mediante `ray-head:10001` y
son intercambiables: todas adjuntan a los actores existentes por nombre
(`get_if_exists`) y enrutan las escrituras al líder. El acceso externo es
`http://localhost:8081`, `http://localhost:8082` y `http://localhost:8083`.
Si falla la conexión inicial, cada API reintenta una vez antes de abortar el
arranque.

La imagen de la API usa `--raft-placement=strict`: si no hay nodos suficientes
para colocar las cinco réplicas, la API se adjunta a las réplicas que ya
funcionan y el supervisor recrea las que falten en su nodo original.

## 5. Comprobar conectividad y tolerancia

```bash
curl http://localhost:8081/health
curl http://localhost:8081/ready
curl -H "Authorization: Bearer $SDCAL_ALICE_TOKEN" \
  http://localhost:8082/v1/status
docker logs sdcal-api-1
```

`/ready` requiere tokens configurados, líder y quórum. Las rutas `/v1/*`
requieren autenticación Bearer. Para probar eventos, disponibilidad y
sincronización frente a todos los pares de contenedores, con recuperación
automática al finalizar, ejecutar:

```bash
./deploy/caos-any-two.sh
```

El estado debe indicar `middleware: ray` y cinco réplicas. Para comprobar la
redundancia, detener dos APIs y dos workers:

```bash
docker stop sdcal-api-2 sdcal-api-3 ray-worker-4 ray-worker-5
curl http://localhost:8081/v1/status
```

La API restante debe seguir confirmando comandos: quedan tres réplicas vivas,
justo el quórum 3/5. Al volver a levantar los workers, el supervisor recrea sus
réplicas desde el SQLite local y estas se ponen al día por log o snapshot.

## 6. Detener el despliegue

```bash
./deploy/down.sh
```

Equivale a:

```bash
docker rm -f $(seq -f 'sdcal-api-%g' 1 3)
docker rm -f $(seq -f 'ray-worker-%g' 1 5)
docker rm -f ray-head
docker volume rm $(seq -f 'sdcal-data-%g' 1 5)
docker volume rm sdcal-gcs-data
docker network rm sdcal
```
