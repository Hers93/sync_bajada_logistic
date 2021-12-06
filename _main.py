import xmltodict
import os
from _dictionary_FM import *
from model import *
import json
from _fields_null import *
import time


def validateDateHourEs(date, type=''):
    for format in ['%Y-%m-%d %H:%M:%S', ' %m/%d/%y', '%d/%m/%Y', '%m/%d/%y', ' %Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%d/%m/%y %H:%M:%S', ' %I:%M:%S %p',
                   '%I:%M:%S %p', '%H:%M:%S', ' %H:%M:%S', '%m/%d/%Y %I:%M:%S %p']:
        try:
            if type == 'F':
                formatoPG = '%Y-%m-%d'
            elif type == 'H':
                formatoPG = '%H:%M:%S'
            else:
                formatoPG = '%m/%d/%Y %I:%M:%S %p'
            time.strptime(str(date)[:19], format)
            return time.strftime(formatoPG, time.strptime(str(date)[:19], format))
        except:
            pass


def _jsondumbs(_json):
    return ("$$" + json.dumps(_json).replace("'", '"').replace('"{', '{').replace('}"', '}') + '$$')


def sanitize(dict=None):
    r_dict = {}
    if dict:
        for e in dict:
            if validateDateHourEs(dict[e]):
                r_dict[e] = validateDateHourEs(dict[e])
            else:
                r_dict[e] = dict[e]
    return r_dict


def get_xml_vise(visitor):
    _result = cnn.getData('vise."idVisitante",vise."idEvento",vise."idEdicion",vise."EncuestaCompleta",vise."Preregistrado",vise."FechaPreregistro"','"AE_LOGISTIC"."VisitanteEdicion" vise', '"idEdicion" = 22 and "idVisitante" = %(idVisitante)s' % visitor)
    print(_result)
    xml_vise = '<visitante_ediciones>'
    if _result:
        for x in _result:
            x = sanitize(x)
            xml_vise += visitor_edition % x
        xml_vise += '</visitante_ediciones>'
    else:
        xml_vise = '<visitante_ediciones/>'
   # xml_vise = xml_vise.replace("'",'"')
   # print(xml_vise)
    # exit()
    return xml_vise


def get_xml_compra(visitor):
    _result = cnn.getData('"idCompra","idVisitante","idEdicion","idCompraStatus","SubTotal","IVA","Total","idFormaPago","idMonedaTipo","ReqFactura","RFC","RazonSocial","EmailFacturacion","AreaCiudad","Telefono","Pais","Estado","CodigoPostal","Colonia","Ciudad","Calle","NumeroExterior","NumeroInterior","FechaPagado","FechaCancelado","CompraGrupal","idCompraPadre","RespuestaBanwireJson"',
                          '"AE"."Compra"', '"idEdicion" = 12 and "idVisitante" = %(idVisitante)s' % visitor)
    print(_result)
    xml_compra = '<Compra>'
    if _result:
        for x in _result:
            x = sanitize(x)
            xml_compra += visitor_compra % x
        xml_compra += '</Compra>'
    else:
        xml_compra = '<Compra/>'
    return xml_compra


def get_xml_comd(visitor):
    _result = cnn.getData('cd."idCompraDetalle",cd."idCompra",cd."Precio",cd."Descuento",cd."PrecioUnitario",cd."idProducto",cd."idProductoTipo",cd."idVisitante"',
                          '"AE"."CompraDetalle" cd inner join "AE"."Compra" c on cd."idCompra" = c."idCompra"', 'c."idEdicion" = 12 and c."idVisitante" = %(idVisitante)s' % visitor)
    print(_result)
    xml_comd = '<Compra_Detalle>'
    if _result:
        for x in _result:
            x = sanitize(x)
            xml_comd += visitor_comd % x
        xml_comd += '</Compra_Detalle>'
    else:
        xml_comd = '<Compra_Detalle/>'
    return xml_comd


def get_xml_visc(visitor):
    _result = cnn.getData('"idCupon","idEdicion","idEvento","idVisitante","idVisitanteCupon"',
                          '"AE"."VisitanteCupon"', '"idEdicion" = 12 and "idVisitante" = %(idVisitante)s' % visitor)
    print(_result)
    xml_visc = '<visitantes_cupon>'
    if _result:
        for x in _result:
            x = sanitize(x)
            xml_visc += visitor_cupon % x
        xml_visc += '</visitantes_cupon>'
    else:
        xml_visc = '<visitantes_cupon/>'
    return xml_visc


def validate_null(xm):
    for l in v:
        xm = xm.replace('<' + l + '>' + 'None' + '</' + l +
                        '>', '<' + l + '/>').replace("u'", '"')

    return xm


if __name__ == "__main__":
    __absolutePath__ = os.path.abspath(os.getcwd())
    if not os.path.exists('xml'):
        os.mkdir('xml')
    if not os.path.exists('logs'):
        os.mkdir('logs')
    while True:
        cnn = conection('10.80.10.10', 'ENCUMEX_SAS','php_FTS17', 'qwpNcaK_y6!Bjc')
        visits = cnn.getData('vis."idVisitante",vis."Email",vis."Nombre",vis."ApellidoPaterno",vis."ApellidoMaterno",vis."DE_Razon_Social",vis."DE_id_Cargo",vis."DE_Cargo",vis."DE_id_Area",vis."DE_Area",vis."DE_WebPage",vis."DE_id_SectorLS",vis."DE_SectorLS",vis."DE_id_GiroLS",vis."DE_GiroLS",vis."DE_id_Pais",vis."DE_Pais",vis."DE_id_Estado",vis."DE_Estado",vis."DE_id_Colonia",vis."DE_Colonia",vis."DE_CP",vis."DE_id_Ciudad",vis."DE_Ciudad",vis."DE_Direccion",vis."DE_AreaCiudad",vis."DE_Telefono",vis."Movil",vis."Tipo_Registro", 22 as "RS_UltimaEdicionPreregistrado"','"AE_LOGISTIC"."Visitante" vis INNER JOIN "AE_LOGISTIC"."VisitanteEdicion" vise ON vis."idVisitante" = vise."idVisitante"', 'vis."SyncPython" = 0 and vise."idEdicion" = 22 limit 50')

        if not visits:
            cnn.closeConection()
            time.sleep(10)
        else:
            xml_vis = '<?xml version="1.0" encoding="UTF-8"?><visitantes>'
            for visit in visits:
                try:
                    visit = sanitize(visit)
                    print(visit)
                    xml_vis += visitor % visit
                    xml_vis += get_xml_vise(visit)
                    print(xml_vis)
                   ## print (xml_vis)
                    # exit()
                    ##xml_vis += get_xml_compra(visit)
                    ##xml_vis += get_xml_comd(visit)
                    ##xml_vis += get_xml_visc(visit)
                    cnn.doUpdate('"AE_LOGISTIC"."Visitante"', '"SyncPython" = 1', '"idVisitante" = %(idVisitante)s' % visit)
                    cnn.committ()
                except Exception as e:
                    os.path.join(__absolutePath__ + '/logs',
                                 "", 'log_general.txt')
                    log = open(__absolutePath__ + '/logs/log_general.txt', 'a')
                    log.write('Ocurrio un error  %s' % e + '\n \n')
                    log.close()
            xml_vis += '</visitantes>'
            xml_vis = validate_null(xml_vis)
            _thisTime = time.strftime("%Y-%m-%d_%H_%M_%S")

            #os.path.join('C:/Users/Herson Mancera/Desktop', "", 'visitor_' + _thisTime + '.txt')
            os.path.join('C:/Users/Desarrollo/Desktop/Pruebas_logistic',
                         "", 'visitor_' + _thisTime + '.txt')
            xml = open(
                'C:/Users/Desarrollo/Desktop/Pruebas_logistic/visitor_' + _thisTime + '.txt',
                'a', encoding='UTF-8')
            # os.path.join('C:/Program Files/FileMaker/FileMaker Server/Data/Documents/Soundcheck/2018/xml', "", 'visitor_'+_thisTime+'.txt')

            # xml = open('C:/Program Files/FileMaker/FileMaker Server/Data/Documents/Soundcheck/2018/xml/visitor_'+_thisTime+'.txt', 'a')
            xml.write(xml_vis.replace('&', '&amp;'))
            xml.close()
            # os.rename('C:/Program Files/FileMaker/FileMaker Server/Data/Documents/Soundcheck/2018/xml/visitor_/visitor_'+_thisTime+'.txt' ,
            # 'C:/Program Files/FileMaker/FileMaker Server/Data/Documents/Soundcheck/2018/xml/visitor_'+_thisTime+'.xml')
            os.rename(
                'C:/Users/Desarrollo/Desktop/Pruebas_logistic/visitor_' + _thisTime + '.txt',
                'C:/Users/Desarrollo/Desktop/Pruebas_logistic/visitor_' + _thisTime + '.xml')
            cnn.closeConection()
