import socket
import struct

def recibir_todo(sock, n_bytes):
    data = b""
    while len(data) < n_bytes:
        paquete = sock.recv(n_bytes - len(data))
        if not paquete:
            raise ConnectionError("Conexión cerrada antes de recibir todos los datos.")
        data += paquete
    return data

ip = "192.168.100.5"
puerto = 8078

cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente.connect((ip, puerto))

#numero identificador
identificador = 100
identificador_by = struct.pack("<I",identificador);

cliente.sendall(identificador_by)

#se manda la ruta de la database
db_ruta = "./Model/DataBases/zomato_DB.db" #ruta relativa de server.o 
db_ruta_bytes = db_ruta.encode()

db_len = len(db_ruta_bytes)
db_len_by = struct.pack("<Q", db_len)

cliente.send(db_len_by)

db_ruta_by = struct.pack(f"<{db_len}s", db_ruta_bytes)

cliente.sendall(db_ruta_by)

#se manda la consulta
consulta = "SELECT Prices FROM Zomato"
consulta_bytes = consulta.encode()

consulta_len = len(consulta_bytes);
consulta_len_by = struct.pack("<Q",consulta_len)

cliente.send(consulta_len_by)

consulta_by = struct.pack(f"<{consulta_len}s",consulta_bytes)

cliente.sendall(consulta_by)

#numero de columnas
columnas = 1
columnas_by = struct.pack("<I",columnas)

cliente.sendall(columnas_by)


#esperando la respuesta

for _ in range(0,columnas):
    tipo_dato_by = cliente.recv(4)
    tipo_dato = struct.unpack("<i",tipo_dato_by)[0]
    print(f"tipo de dato es {tipo_dato}")

    longitud_by = cliente.recv(8)
    longitud = struct.unpack("<Q",longitud_by)[0]
    print(f"longitud del array {longitud}")

    match tipo_dato:
        case 1:
            data_array = recibir_todo(cliente,longitud*8)
            array = struct.unpack(f"<{longitud}Q",data_array)
            print(array)
        case 2:
            data_array_double = recibir_todo(cliente,longitud*8)
            array_double = struct.unpack(f"<{longitud}d",data_array_double)
            print(array_double)
        case 3:
            array_string = []

            for i in range(0,longitud):
                len_string_by = cliente.recv(8)
                len_string = struct.unpack("<Q",len_string_by)[0]

                palabra_by = recibir_todo(cliente,len_string)
                palabra = struct.unpack(f"<{len_string}s",palabra_by)
                palabra = palabra_by.decode('utf-8')
                array_string.append(palabra)
                
            print(array_string)



cliente.close()
