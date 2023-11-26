from pydantic import BaseModel


class Request_Animal(BaseModel):
    id:                  int                 
    dt_nascimento:       str        
    raca:                str                
    nome:                str                 
    sexo:                str
    idade:               int 
    categoria:           str
    id_fazenda:          int

class Request_Fazendeiro(BaseModel):
    idFazendeiro:       int                 
    dt_nascimento:      str        
    nome:               str                
    sexo:               str
    endereco:           str 
    contato:            str
    email:              str
    senha:              str

class Request_Fazenda(BaseModel):
    idFazenda:          int
    nome:               str
    endereco:           str
    idFazendeiro:       int

class Request_Ordenha(BaseModel):
    idOrdenha:          int
    qtdLeite:           float
    dataOrdenha:        str
    idAnimal:           int

class Request_Pesagem(BaseModel):
    idPesagem:          int
    peso:               float
    dataPesagem:        str
    idAnimal:           int