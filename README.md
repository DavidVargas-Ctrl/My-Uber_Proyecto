
# **Sistema de Gestión de Taxis y Usuarios con ZeroMQ y MQTT**

Este proyecto implementa un sistema distribuido de gestión de taxis y usuarios. Los taxis se comunican con un servidor central usando el protocolo MQTT, mientras que los usuarios interactúan con el servidor mediante ZeroMQ. El sistema simula solicitudes de taxis y asigna los más cercanos a los usuarios.

## **Características del Proyecto**

- **Servidor Central**: Coordina las solicitudes de taxis y su asignación eficiente.
- **Taxis**: Simulan movimientos dentro de una cuadrícula y responden a solicitudes de usuarios.
- **Usuarios**: Generan solicitudes de taxis al servidor con posiciones específicas.
- **Comunicaciones**:
  - **MQTT**: Usado por los taxis para reportar su posición y recibir asignaciones.
  - **ZeroMQ**: Usado por los usuarios para solicitar taxis al servidor.

---

## **Requisitos**

Antes de ejecutar el proyecto, asegúrate de tener instalados los siguientes componentes:

### **Dependencias**
1. **Python 3.9+**
2. **Librerías Python**:
   - `paho-mqtt`
   - `pyzmq`

Puedes instalar las dependencias ejecutando:

```bash
pip install paho-mqtt pyzmq
```

---

## **Estructura del Proyecto**

```
.
├── server.py         # Código del servidor central
├── taxi.py           # Código del proceso para taxis
├── users.py          # Código para simular usuarios
├── coordenadas_Usuarios.txt  # Archivo de coordenadas de usuarios
├── Interaccion.txt   # Archivo generado con el estado del sistema
```

---

## **Ejecución**

### **1. Iniciar el Servidor**

El servidor central coordina el sistema. Para ejecutarlo, utiliza el siguiente comando:

```bash
python3 server.py --cuadricula_N <N> --cuadricula_M <M> [--intervalo_guardado <segundos>]
```

#### **Ejemplo**:

```bash
python3 server.py --cuadricula_N 50 --cuadricula_M 50 --intervalo_guardado 10
```

- `--cuadricula_N` y `--cuadricula_M`: Dimensiones de la cuadrícula.
- `--intervalo_guardado`: Tiempo (en segundos) para guardar el estado del sistema (opcional, por defecto es 60).

### **2. Iniciar los Taxis**

Cada taxi se ejecuta como un proceso independiente. Usa el siguiente comando para iniciarlo:

```bash
python3 taxi.py --taxi_id <id> --cuadricula_N <N> --cuadricula_M <M> --init_x <x> --init_y <y> --velocidad <velocidad>
```

#### **Ejemplo**:

```bash
python3 taxi.py --taxi_id 1 --cuadricula_N 50 --cuadricula_M 50 --init_x 5 --init_y 5 --velocidad 2
```

- `--taxi_id`: Identificador único del taxi.
- `--cuadricula_N` y `--cuadricula_M`: Dimensiones de la cuadrícula.
- `--init_x` y `--init_y`: Posición inicial del taxi.
- `--velocidad`: Velocidad en km/h (valores permitidos: 1, 2 o 4).

### **3. Simular Usuarios**

Ejecuta el proceso para generar usuarios y enviar solicitudes al servidor:

```bash
python3 users.py --num_usuarios <Y> --cuadricula_N <N> --cuadricula_M <M> --coords <archivo> --server_address <address>
```

#### **Ejemplo**:

```bash
python3 users.py --num_usuarios 3 --cuadricula_N 50 --cuadricula_M 50 --coords coordenadas_Usuarios.txt --server_address 192.168.0.9
```

- `--num_usuarios`: Número de usuarios a generar.
- `--cuadricula_N` y `--cuadricula_M`: Dimensiones de la cuadrícula.
- `--coords`: Archivo con coordenadas de los usuarios.
- `--server_address`: Dirección IP del servidor ZeroMQ.

---

## **Formato del Archivo de Coordenadas**

El archivo de coordenadas debe tener el siguiente formato:

```
x y tiempo_min
10 20 5
15 30 10
5 10 3
```

- `x`: Coordenada inicial del usuario en el eje X.
- `y`: Coordenada inicial del usuario en el eje Y.
- `tiempo_min`: Tiempo (en minutos simulados) que el usuario esperará antes de solicitar un taxi.

---

## **Resultados**

El sistema genera un archivo `Interaccion.txt` que contiene:

- Registro de taxis y usuarios.
- Resumen de servicios exitosos y rechazados.
- Información sobre asignaciones y tiempos de respuesta.

---
