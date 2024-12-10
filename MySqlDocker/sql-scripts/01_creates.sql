-- Crear la tabla CLIENTE
CREATE TABLE CLIENTE (
    ID_CLIENTE VARCHAR(2) PRIMARY KEY,       -- ID del cliente (clave primaria, autoincrementable)
    DES_X INT NOT NULL,                          -- Coordenadas del destino
	DES_Y INT NOT NULL,
    ESTADO VARCHAR(255) NOT NULL,                    -- Estado del cliente (por ejemplo, "esperando", "en ruta", etc.)
    POS_X INT NOT NULL,                        -- Coordenadas actuales del taxi
	POS_Y INT NOT NULL
);

-- Crear la tabla TAXI
CREATE TABLE TAXI (
    ID_TAXI INT PRIMARY KEY,         -- ID del taxi (clave primaria, autoincrementable)
    POS_X INT  DEFAULT 0 NOT NULL,                        -- Coordenadas actuales del taxi
	POS_Y INT  DEFAULT 0 NOT NULL,
    ESTADO VARCHAR(255)  DEFAULT 'SinConexion' NOT NULL,                    -- Estado del taxi (por ejemplo, "disponible", "ocupado", etc.)
    CONECTADO BOOLEAN  DEFAULT 0 NOT NULL,                      -- Indica si el taxi está conectado o no (BOOLEANO)
    ID_CLIENTE VARCHAR(2),                                  -- ID del cliente asociado al taxi (relación con CLIENTE)
    PASSWORD VARCHAR(255) NOT NULL,
    FOREIGN KEY (ID_CLIENTE) REFERENCES CLIENTE(ID_CLIENTE)  -- Relación con la tabla CLIENTE
);

-- Crear la tabla TAXI
CREATE TABLE UBICACIONES (
    ID_UBICACION VARCHAR(2) PRIMARY KEY,         -- ID del taxi (clave primaria, autoincrementable)
    POS_X INT NOT NULL,                        -- Coordenadas actuales del taxi
	POS_Y INT NOT NULL
);

CREATE TABLE TOKENS (
    ID INT PRIMARY KEY,
    TOKEN VARCHAR(255) NOT NULL,
    EXPIRATION DATETIME NOT NULL
);