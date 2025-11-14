# Cómo levantar el proyecto

Este proyecto se ejecuta utilizando **Docker Compose**, tal como se
describe en el informe del trabajo práctico.

## Requisitos

Asegurate de tener Docker Compose instalado.\
Si no lo tenés:

``` bash
sudo apt install docker-compose
```

------------------------------------------------------------------------

## Puesta en marcha

Desde la **raíz del proyecto**, ejecutar:

### 1. Levantar los servicios (app, MongoDB y Redis)

**Primera vez o para forzar build:**

``` bash
docker-compose up -d --build
```

**Si ya fue buildeado previamente:**

``` bash
docker-compose up -d
```

------------------------------------------------------------------------

### 2. Ingresar al contenedor de la aplicación

``` bash
docker exec -it app bash
```

------------------------------------------------------------------------

### 3. Cargar los datos iniciales

Dentro del contenedor, ejecutar el cargador de datos apuntando a la
carpeta `/app/data`:

``` bash
python data_loader.py /app/data
```

Este paso es obligatorio para inicializar MongoDB con los datos
requeridos por la aplicación.

------------------------------------------------------------------------

### 4. Ejecutar la aplicación principal

Una vez cargados los datos:

``` bash
python main.py
```

La aplicación mostrará un menú de opciones con las consultas numeradas
según la consigna del trabajo práctico.

------------------------------------------------------------------------

## Parar los contenedores

Para detener todos los servicios:

``` bash
docker-compose down
```
