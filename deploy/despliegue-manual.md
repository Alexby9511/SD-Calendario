# Despliegue manual de SD Calendario sobre Ray

Cada tipo de nodo se construye y se arranca con su propio `Dockerfile` y un
`docker run`.

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
docker run -d --name ray-worker-1 --network sdcal sdcal-ray-worker \
  ray start --address=ray-head:6379 --block

docker run -d --name ray-worker-2 --network sdcal sdcal-ray-worker \
  ray start --address=ray-head:6379 --block
```

Comprobar que ambos contenedores siguen activos:

```bash
docker ps --filter name=ray-
```

## 4. Arrancar la API

```bash
docker run -d --name sdcal-api --network sdcal -p 8081:8080 \
  sdcal-ray-api \
  python -m server.cmd.sdcald \
  --ray-address=ray://ray-head:10001 --host=0.0.0.0 --port=8080
```

La API se conecta al Ray Client del head mediante `ray-head:10001`. El acceso
externo es `http://localhost:8081`.

## 5. Comprobar conectividad

```bash
curl http://localhost:8081/health
curl http://localhost:8081/v1/status
docker logs sdcal-api
```

El estado debe indicar `middleware: ray`. La API, el head y los workers se
comunican exclusivamente a través de Ray y de la red Docker privada.

## 6. Detener el despliegue

```bash
docker rm -f sdcal-api ray-worker-2 ray-worker-1 ray-head
docker network rm sdcal
```
