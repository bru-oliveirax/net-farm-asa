from fastapi import FastAPI
from classes import Request_Animal, Request_Fazenda, Request_Fazendeiro, Request_Ordenha, Request_Pesagem
from models import Animal,Fazenda,Fazendeiro,Pesagem, Ordenha, session
from publisher import Publisher
import logging


logger = logging.getLogger(__name__)

logging.getLogger("pika").propagate = False
FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)

app = FastAPI()

config = {
     'host'     : '172.17.0.4',
     'port'     : 5672, 
     'exchange' : 'animais'
}

# --------------------------------- ROOT  ---------------------------------
@app.get("/")
async def root():
    return {
        "status": "SUCESS",
        "data"  : "NO DATAS"
    }


# ------------------------------  ANIMAIS  --------------------------------
@app.get("/animal")
async def get_all_animal():
    animais_query = session.query(Animal)
    animais = animais_query.all()
    return {
        "status": "SUCESS",
        "data"  : animais
    }


@app.put("/animais")
async def alterar_animal(request_animal: Request_Animal):
    try:    
        animal_json = request_animal
        animal_query = session.query(Animal).filter(
            Animal.id==animal_json.id
        )
        animal = animal_query.first()

        animal.nome          = animal_json.nome
        animal.raca          = animal_json.raca
        animal.sexo          = animal_json.sexo
        animal.categoria     = animal_json.categoria
        animal.idade         = animal_json.idade
        animal.id_fazenda    = animal_json.id_fazenda
        animal.dt_nascimento = animal_json.dt_nascimento

        session.add(animal)
        session.commit()

        return {
            "status": "SUCESS",
            "data"  : animal_json
        }
    
    except Exception as e:
            return {
                "status": "SUCESS",
                "data"  : "ANIMAL NÃO ENCONTRADO"
            }


@app.post("/animais")
async def criar_animal(request_animal: Request_Animal):
    animal_json = request_animal

    animal = Animal(
        nome       = animal_json.nome,
        raca       = animal_json.raca,
        sexo       = animal_json.sexo,
        categoria  = animal_json.categoria,
        idade      = animal_json.idade,
        id_fazenda = animal_json.id_fazenda

    )
    session.add(animal)
    session.commit()

    return {
        "status": "SUCESS",
        "data"  : animal_json
    }


@app.get("/enviar_animais", status_code=200)
async def get_all_animais():
    animais_to_send = []
    logger.info('Coletando as informações dos animais no banco de dados')
    
    try:
        animais_query = session.query(Animal)
        animais = animais_query.all()

        for animal in animais:
            item = {
                "id"           : animal.id,
                "nome"         : animal.nome,
                "raca"         : animal.raca,
                "sexo"         : animal.sexo,
                "dt_nascimento": animal.dt_nascimento,
                "categoria"    : animal.categoria,
                "idade"        : animal.idade,
                "id_fazenda"   : animal.id_fazenda
            }
            animal_serializer = Request_Animal(**item)
            animais_to_send.append(animal_serializer)
        
        publisher = Publisher(config)  
        logger.info('Enviando mensagem para o RabbitMQ')       
        publisher.publish('routing_key', animal_serializer.model_dump_json().encode())
         
    except Exception as e:
         logger.error(f'Erro na consulta dos animais -- get_all_animais() -- {e}')
         print(e)
    return {
        "status": "SUCESS",
        "result": "OK"
    }


# ----------------------------  FAZENDEIROS  ------------------------------
@app.get("/fazendeiros")
async def get_all_fazendeiro():
    fazendeiros_query = session.query(Fazendeiro)
    fazendeiros = fazendeiros_query.all()
    return {
        "status": "SUCESS",
        "data"  : fazendeiros
    }


@app.put("/fazendeiros")
async def alterar_fazendeiro(request_fazendeiro: Request_Fazendeiro):
    try:    
        fazendeiro_json = request_fazendeiro
        fazendeiro_query = session.query(Fazendeiro).filter(
            Fazendeiro.idFazendeiro ==fazendeiro_json.idFazendeiro
        )
        fazendeiro = fazendeiro_query.first()

        fazendeiro.nome          = fazendeiro_json.nome
        fazendeiro.dt_nascimento = fazendeiro_json.dt_nascimento
        fazendeiro.sexo          = fazendeiro_json.sexo
        fazendeiro.endereco      = fazendeiro_json.endereco
        fazendeiro.contato       = fazendeiro_json.contato
        fazendeiro.senha         = fazendeiro_json.senha
        fazendeiro.email         = fazendeiro_json.email
                
        session.add(fazendeiro)
        session.commit()

        return {
            "status": "SUCESS",
            "data"  : fazendeiro_json
        }
    
    except Exception as e:
            return {
                "status": "SUCESS",
                "data"  : "FAZENDEIRO NÃO ENCONTRADO"
            }

@app.post("/fazendeiros")
async def criar_fazendeiro(request_fazendeiro: Request_Fazendeiro):
    fazendeiro_json = request_fazendeiro

    fazendeiro = Fazendeiro(
        nome          = fazendeiro_json.nome,
        dt_nascimento = fazendeiro_json.dt_nascimento,
        sexo          = fazendeiro_json.sexo,
        endereco      = fazendeiro_json.endereco,
        contato       = fazendeiro_json.contato,
        senha         = fazendeiro_json.senha,
        email         = fazendeiro_json.email
    )
    session.add(fazendeiro)
    session.commit()

    return {
        "status": "SUCESS",
        "data"  : fazendeiro_json
    }

@app.get("/enviar_fazendeiros", status_code=200)
async def get_all_fazendeiros():
    fazendeiros_to_send = []
    logger.info('Coletando as informações dos fazendeiros no banco de dados')
    
    try:
        fazendeiros_query = session.query(Fazendeiro)
        fazendeiros = fazendeiros_query.all()

        for fazendeiro in fazendeiros:
            item = {
                "idFazendeiro" : fazendeiro.idFazendeiro,
                "nome"         : fazendeiro.nome,
                "dt_nascimento": fazendeiro.dt_nascimento,
                "sexo"         : fazendeiro.sexo ,
                "endereco"     : fazendeiro.endereco ,
                "contato"      : fazendeiro.contato ,
                "email"        : fazendeiro.email,
                "senha"        : fazendeiro.senha
            }
            fazendeiro_serializer = Request_Fazendeiro(**item)
            fazendeiros_to_send.append(fazendeiro_serializer)
        
        publisher = Publisher(config)  
        logger.info('Enviando mensagem para o RabbitMQ')       
        publisher.publish('routing_key', fazendeiro_serializer.model_dump_json().encode())

    except Exception as e:
         logger.error(f'Erro na consulta dos fazendeiros -- get_all_fazendeiros() -- {e}')
         print(e)
    return {
        "status": "SUCESS",
        "result": "OK"
    }


# ------------------------------  FAZENDA  --------------------------------
@app.get("/fazendas")
async def get_all_fazendas():
    fazendas_query = session.query(Fazenda)
    fazendas = fazendas_query.all()
    return {
        "status": "SUCESS",
        "data"  : fazendas
    }


@app.put("/fazendas")
async def alterar_fazenda(request_fazenda: Request_Fazenda):
    try:    
        fazenda_json = request_fazenda
        fazenda_query = session.query(Fazenda).filter(
            Fazenda.idFazenda==fazenda_json.idFazenda
        )
        fazenda = fazenda_query.first()

        fazenda.idFazenda    = fazenda_json.idFazenda
        fazenda.nome         = fazenda_json.nome 
        fazenda.endereco     = fazenda_json.endereco
        fazenda.idFazendeiro = fazenda_json.idFazendeiro

        session.add(fazenda)
        session.commit()

        return {
            "status": "SUCESS",
            "data"  : fazenda_json
        }
    
    except Exception as e:
            return {
                "status": "SUCESS",
                "data"  : "FAZENDA NÃO ENCONTRADO"
            }


@app.post("/fazendas")
async def criar_fazenda(request_fazenda: Request_Fazenda):
    fazenda_json = request_fazenda

    fazenda = Fazenda(
        nome         = fazenda_json.nome,
        endereco     = fazenda_json.endereco,
        idFazendeiro = fazenda_json.idFazendeiro
    )
    session.add(fazenda)
    session.commit()

    return {
        "status": "SUCESS",
        "data"  : fazenda_json
    }

@app.get("/enviar_fazendas", status_code=200)
async def get_all_fazendas():
    fazendas_to_send = []
    logger.info('Coletando as informações dos fazendas no banco de dados')
    
    try:
        fazendas_query = session.query(Fazenda)
        fazendas = fazendas_query.all()

        for fazenda in fazendas:
            item = {
                "idFazenda"   : fazenda.idFazenda,
                "nome"        : fazenda.nome,
                "endereco"    : fazenda.endereco,
                "idFazendeiro": fazenda.idFazendeiro
            }
            fazenda_serializer = Request_Fazenda(**item)            
            fazendas_to_send.append(fazenda_serializer)
        
        publisher = Publisher(config)  
        logger.info('Enviando mensagem para o RabbitMQ')       
        publisher.publish('routing_key', fazenda_serializer.model_dump_json().encode())
    
    except Exception as e:
         logger.error(f'Erro na consulta das fazendas -- get_all_fazendas() -- {e}')
         print(e)
    return {
        "status": "SUCESS",
        "result": "OK"
    }


# ------------------------------  ORDENHA  --------------------------------
@app.get("/ordenhas")
async def get_all_ordenhas():
    ordenhas_query = session.query(Ordenha)
    ordenhas = ordenhas_query.all()
    return {
        "status": "SUCESS",
        "data"  : ordenhas
    }


@app.put("/ordenhas")
async def alterar_ordenha(request_ordenha: Request_Ordenha):
    try:    
        ordenha_json = request_ordenha
        ordenha_query = session.query(Ordenha).filter(
            Ordenha.idOrdenha==ordenha_json.idOrdenha
        )
        ordenha = ordenha_query.first()

        ordenha.qtdLeite = ordenha_json.qtdLeite
        ordenha.dataOrdenha = ordenha_json.dataOrdenha
        ordenha.idAnimal = ordenha_json.idAnimal

        session.add(ordenha)
        session.commit()

        return {
            "status": "SUCESS",
            "data"  : ordenha_json
        }
    
    except Exception as e:
            return {
                "status": "SUCESS",
                "data": "ORDENHA NÃO ENCONTRADA"
            }


@app.post("/ordenhas")
async def criar_ordenha(request_ordenha: Request_Ordenha):
    ordenha_json = request_ordenha

    ordenha = Ordenha(
        qtdLeite    = ordenha_json.qtdLeite,
        dataOrdenha = ordenha_json.dataOrdenha,
        idAnimal    = ordenha_json.idAnimal
    )
    session.add(ordenha)
    session.commit()

    return {
        "status": "SUCESS",
        "data"  : ordenha_json
    }


@app.get("/enviar_ordenhas", status_code=200)
async def get_all_ordenhas():
    ordenhas_to_send = []
    logger.info('Coletando as informações das ordenhas no banco de dados')
    
    try:
        ordenhas_query = session.query(Ordenha)
        ordenhas = ordenhas_query.all()
        
        for ordenha in ordenhas:
            item = {
                "idOrdenha"  : ordenha.idOrdenha,
                "qtdLeite"   : ordenha.qtdLeite,
                "dataOrdenha": ordenha.dataOrdenha,
                "idAnimal"   : ordenha.idAnimal
            }
            ordenha_serializer = Request_Ordenha(**item)            
            ordenhas_to_send.append(ordenha_serializer)
        
        publisher = Publisher(config)  
        logger.info('Enviando mensagem para o RabbitMQ')       
        publisher.publish('routing_key', ordenha_serializer.model_dump_json().encode())

    except Exception as e:
         logger.error(f'Erro na consulta dos ordenhas -- get_all_ordenhas() -- {e}')
         print(e)
    return {
        "status": "SUCESS",
        "result": "OK"
    }


# ------------------------------  PESAGEM  --------------------------------
@app.get("/pesagem")
async def get_all_pesagem():
    pesagens_query = session.query(Pesagem)
    pesagens = pesagens_query.all()
    return {
        "status": "SUCESS",
        "data"  : pesagens
    }


@app.put("/pesagens")
async def alterar_pesagem(request_pesagem: Request_Pesagem):
    try:    
        pesagem_json = request_pesagem
        pesagem_query = session.query(Pesagem).filter(
            Pesagem.idPesagem==pesagem_json.idPesagem
        )
        pesagem = pesagem_query.first()

        pesagem.peso        = pesagem_json.peso
        pesagem.dataPesagem = pesagem_json.dataPesagem
        pesagem.idAnimal    = pesagem_json.idAnimal

        session.add(pesagem)
        session.commit()

        return {
            "status": "SUCESS",
            "data": pesagem_json
        }
    
    except Exception as e:
            return {
                "status": "SUCESS",
                "data": "pesagem NÃO ENCONTRADO"
            }


@app.post("/pesagens")
async def criar_pesagem(request_pesagem: Request_Pesagem):
    pesagem_json = request_pesagem

    pesagem = Pesagem(
        peso        = pesagem_json.peso,
        dataPesagem = pesagem_json.dataPesagem,
        idAnimal    = pesagem_json.idAnimal

    )
    session.add(pesagem)
    session.commit()

    return {
        "status": "SUCESS",
        "data"  : pesagem_json
    }


@app.get("/enviar_pesagens", status_code=200)
async def get_all_pesagens():
    pesagens_to_send = []
    logger.info('Coletando as informações dos pesagens no banco de dados')
    
    try:
        pesagens_query = session.query(Pesagem)
        pesagens = pesagens_query.all()

        for pesagem in pesagens:
            item = {
                "idPesagem"  : pesagem.idPesagem,
                "peso"       : pesagem.peso,
                "dataPesagem": pesagem.dataPesagem,
                "idAnimal"   : pesagem.idAnimal
            }
            pesagem_serializer = Request_Pesagem(**item)            
            pesagens_to_send.append(pesagem_serializer)
        
        publisher = Publisher(config)  
        logger.info('Enviando mensagem para o RabbitMQ')       
        publisher.publish('routing_key', pesagem_serializer.model_dump_json().encode())
    
    except Exception as e:
         logger.error(f'Erro na consulta dos pesagens -- get_all_pesagens() -- {e}')
         print(e)
    return {
        "status": "SUCESS",
        "result": "OK"
    }