# 🚀 Cómo levantar el proyecto

Este proyecto se ejecuta utilizando **Docker Compose**.

## 📦 Requisitos

Asegurate de tener Docker Compose instalado.\
Si no lo tenés:

``` bash
sudo apt install docker-compose
```

## ▶️ Puesta en marcha

Desde la **raíz del proyecto**, ejecutá:

### Levantar los contenedores

**Primera vez (o si querés forzar build):**

``` bash
docker-compose up -d --build
```

**Si ya lo buildeaste antes:**

``` bash
docker-compose up -d
```

### Ingresar al contenedor principal

``` bash
docker exec -it app bash
```

### Ejecutar la aplicación

Dentro del contenedor:

``` bash
python main.py
```

## ⛔ Parar los contenedores

``` bash
docker-compose down
```
