class Vehículo():

    def __init__(self,color, ruedas):
        self.color = color
        self.ruedas = ruedas

    def __str__(self):
        return "color {},{} ruedas".format(self.color, self.ruedas)

#Definimos la Clase Coche 
class Coche(Vehículo): #Proviene de la Clase Vehículo
    
    def __init__(self, color, ruedas, velocidad, cilindrada):
        super().__init__(color,ruedas) #Lo traemos de la superclase Vehículo
        self.velocidad = velocidad
        self.cilindrada = cilindrada

    def __str__(self):
        return super().__str__() + ", {} km/h, {} cc".format(self.velocidad,self.cilindrada)
    
#Definimos la Clase Camioneta 
class Camioneta(Coche): #Proviene de la clase Coche

    def __init__(self, color, ruedas, velocidad, cilindrada, carga):
        super().__init__(color,ruedas, velocidad, cilindrada)
        self.carga = carga

    def __str__(self):
        return super().__str__() + ", {} kg".format(self.carga)

#Definimos la Clase Bicicleta 
class Bicicleta(Vehículo): #Proviene de la clase Vehículo

    def __init__(self, color, ruedas, tipo):
        super().__init__(color, ruedas)
        self.tipo = tipo
    
    def __str__(self):
        return super().__str__() + ", {}".format(self.tipo)

#Definimos clase Motocicleta
class Motocilcleta(Bicicleta): #Proviene de Bicicleta

    def __init__(self, color, ruedas, tipo,velocidad, cilindrada):
        super().__init__(color, ruedas, tipo)
        self.velocidad = velocidad
        self.cilindrada = cilindrada

    def __str__(self):
        return super().__str__() + ", {} km/h, {} cc".format(self.velocidad, self.cilindrada)

#Creamos la lista de Vehículos que luego recorreremos
vehiculos = [
    Coche("Azul",4 , 150, 1200),
    Camioneta("Blanca", 4, 120, 1300, 1500), 
    Bicicleta("Roja", 2, "Urbana"),
    Motocilcleta("Negro", 2, "Deportiva", 140, 900)
]

#Creamos la función catalogar

'''
def catalogar(lista):
    for v in lista:
        print (f'{type(v).__name__}, {v}')

catalogar(vehiculos)
'''

#Modificamos la función catalogar
def catalogar(lista, ruedas = None):
    if ruedas != None:
        contador = 0
        for v in vehiculos: #Con un bucle for, recorremos los vehículos para imprimir el número de vehículos encontrados
            if  v.ruedas == ruedas:
                contador +=1
        print (f'Se han encontrado {contador} vehículos con {ruedas} ruedas') 
        print("--------------------------------")
    for v in lista:
        if ruedas == None:
            print (f'{type(v).__name__}, {v}')
        else: #Si el numero de ruedas, no es None, imprimirá los vehículos que tengan ese número de ruedas
            if v.ruedas == ruedas:
                print (f'{type(v).__name__}, {v}')

catalogar(vehiculos)

