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

ip = "192.168.0.222"
puerto = 8078

cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente.connect((ip, puerto))

#numero identificador
identificador = 200
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
consulta = "SELECT Dining_Rating,Delivery_Votes,Prices FROM Zomato"
consulta_bytes = consulta.encode()

consulta_len = len(consulta_bytes);
consulta_len_by = struct.pack("<Q",consulta_len)

cliente.send(consulta_len_by)

consulta_by = struct.pack(f"<{consulta_len}s",consulta_bytes)

cliente.sendall(consulta_by)

#numero de columnas
columnas = 3
columnas_by = struct.pack("<I",columnas)

cliente.sendall(columnas_by)


#recibiendo los datos

for i in range(0,columnas):
    #tipo de dato
    tipo_by = cliente.recv(4)
    tipo = struct.unpack("<i",tipo_by)[0]

    #media
    media_by = cliente.recv(8)
    media = struct.unpack("<d",media_by)[0]

    mediana = 0
    moda = 0

    match tipo:
        case 1:
            #mediana
            mediana_by = cliente.recv(8)
            mediana = struct.unpack("<q",mediana_by)[0]

            #moda
            moda_by =  cliente.recv(8)
            moda = struct.unpack("<q",moda_by)[0]
        case 2:
            #mediana
            mediana_by = cliente.recv(8)
            mediana = struct.unpack("<d",mediana_by)[0]

            #moda
            moda_by =  cliente.recv(8)
            moda = struct.unpack("<d",moda_by)[0]
  
    print(f"tipo de dato {tipo}")
    print(f"la media es de {media}")
    print(f"la mediana es {mediana}")
    print(f"la moda es de {moda}")

cliente.close();
