# Despliegue manual de SD Calendario sobre Ray

Cada tipo de nodo se construye y se arranca con su propio `Dockerfile` y un
`docker run`. El despliegue de referencia usa 1 head, 5 workers y 3 APIs sin
estado: 5 réplicas Raft, una por nodo, con quórum 3/5.

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
docker run -d --name ray-head --network sdcal \
  -p 6379:6379 -p 8265:8265 \
  sdcal-ray-head \
  ray start --head --port=6379 --ray-client-server-port=10001 \
  --dashboard-host=0.0.0.0 --dashboard-port=8265 --block
```

Esperar a que el head esté listo antes de arrancar los workers:

```bash
docker logs ray-head
```

## 3. Arrancar los workers

```bash
for index in 1 2 3 4 5; do
  docker run -d --name "ray-worker-$index" --network sdcal sdcal-ray-worker \
    ray start --address=ray-head:6379 --block
done
```

Comprobar que los cinco contenedores siguen activos:

```bash
docker ps --filter name=ray-
```

Con 5 workers y el head, el planificador estricto coloca una réplica Raft en
cada nodo worker y deja el head para el supervisor. Si un worker cae, el
supervisor espera a que vuelva para recrear la réplica sobre su SQLite local;
no la mueve a otro nodo.

## 4. Arrancar las APIs

```bash
for index in 1 2 3; do
  port=$((8080 + index))
  docker run -d --name "sdcal-api-$index" --network sdcal -p "$port:8080" \
    sdcal-ray-api
done
```

Las tres APIs se conectan al Ray Client del head mediante `ray-head:10001` y
son intercambiables: todas adjuntan a los actores existentes por nombre
(`get_if_exists`) y enrutan las escrituras al líder. El acceso externo es
`http://localhost:8081`, `http://localhost:8082` y `http://localhost:8083`.

## 5. Comprobar conectividad y tolerancia

```bash
curl http://localhost:8081/health
curl http://localhost:8082/v1/status
curl http://localhost:8083/v1/status
docker logs sdcal-api-1
```

El estado debe indicar `middleware: ray` y cinco réplicas. Para comprobar la
redundancia, detener dos APIs y dos workers:

```bash
docker rm -f sdcal-api-2 sdcal-api-3 ray-worker-4 ray-worker-5
curl http://localhost:8081/v1/status
```

La API restante debe seguir confirmando comandos: quedan tres réplicas vivas,
justo el quórum 3/5. Al volver a levantar los workers, el supervisor recrea sus
réplicas desde el SQLite local y estas se ponen al día por log o snapshot.

## 6. Detener el despliegue

```bash
docker rm -f $(seq -f 'sdcal-api-%g' 1 3)
docker rm -f $(seq -f 'ray-worker-%g' 1 5)
docker rm -f ray-head
docker network rm sdcal
```
