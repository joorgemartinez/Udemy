# FAST API

## Qué vamos a aprender?
En esta unidad vamos a aprender cómo crear una API web para nuestro proyecto gestor de clientes utilizando el módulo Fast Api. 

## Qué es una API?

- Las APIs son interfaces que permiten comunicar programas creados en diferentes arquitecturas entre ellos. 

- En nuestro caso el objetivo es manipular los clientes a través del navegador web mediante la implementación de las acciones de consulta, creación, modificación y borrado. 

- Para terminar, también veremos cómo comunicarnos con la API desde código Python utilizando el famoso módulo *Request*

## Configuración Previa

Desde nuestro proyecto, situados en la raíz del directorio "gestor", vamos a crear un entorno virtual para empezar a trabajar. 

En él vamos a instalar 3 módulos: 

- El primero es Fast Api, que es el módulo de desarrollo ágil de APIs


- Luego instalaremos un servidor web ASGI, que es un servidor de tipo asíncrono llamado Uvicorn, que es con el que se recomienda trabajar con Fast Api, que nos permitirá servir de forma asíncrona a nuestra API para poder hacer pruebas con ella. 


- Y finalmente, un módulo que se utiliza en Fast Api para crear la plantilla de los objetos. 

```
pipenv install fastapi uvicorn pydantic
```

- Otra cosa que podemos hacer mediante pipenv es instalar requirements, qye cintiene el módulo pytest. Así descartamos que haya algun problema con el gestor. 

```
pipenv install -r requirements.txt
```

```
pipenv run pytest -v
```