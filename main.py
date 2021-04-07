from flask import Flask, request
from flask_restful import Api, Resource
import base64
import os
import tempfile
import json
from Recognize import recognzie
import subprocess
import sys
import datetime
import concurrent.futures as conc
from sys import platform

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.config['RESTFUL_JSON'] = {'ensure_ascii': False}
api = Api(app)

ImageMagick = r'C:\Program Files\ImageMagick-7.0.10-Q16'
gs = r'C:\Program Files\gs\gs9.53.3\bin\gswin64c.exe'

class Quote(Resource):
    def get(self, id=0):
        return f"must use POST method", 200

    def post(self, id=0):
            
        try:
            files = request.get_json()
            if type(files) != list:
                return f"POST data must be JSON: list of objects with fields 'file','id','extension','id_file'", 201
        except:
            return f"POST data must be JSON: list of objects with fields 'file','id','extension','id_file'", 201
        result = []
        for _file in files:
            if type(_file) != dict:
                result.append({'error': r"POST data must be JSON objects with fields 'file','id','extension','id_file'"})
                continue
            if _file.get('file') == None or _file.get('id') == None or _file.get('extension') == None or _file.get('id_file') == None:
                result.append({'error': r"POST data must be JSON objects with fields 'file','id','extension','id_file'"})
                continue
            try:
                data = base64.b64decode(_file["file"])
            except:
                result.append({'id_file': _file["id_file"],'error': r"file must be in base64"})

            fhandle, fname = tempfile.mkstemp(suffix='.'+str(_file["extension"]), dir=tempfile.gettempdir())
            try:
                with open(fname, 'wb') as f:
                    f.write(data)
                os.close(fhandle)
            except:
                os.close(fhandle)
                os.remove(fname)
                result.append({'id_file': _file["id_file"],'error': r"error save file"})
            try:
                FileResult = []
                if _file["extension"].upper() == 'PDF':
                    fhandle_pdf, fname_pdf = tempfile.mkstemp(suffix='',dir=tempfile.gettempdir())
                    os.close(fhandle_pdf)
                    os.remove(fname_pdf)
                    out = fname_pdf+'%02d.jpg'
                    if platform == "linux" or platform == "linux2":
                        cmd = 'gs -dBATCH -dNOPAUSE -sDEVICE=jpeg -r500 -dSAFER -sOutputFile='+out+' '+fname
                        os.system(cmd)
                    else:
                        cmd = '"' + gs + '" -dBATCH -dNOPAUSE -sDEVICE=jpeg -r500 -dSAFER -sOutputFile="' + out + '" "' + fname + '"'
                        subprocess.check_output(cmd)

                    i = 1
                    executor = conc.ThreadPoolExecutor(5)
                    futures = []
                    max_kol = 5
                    tek_result = []
                    spis = []

                    while True:
                        path = fname_pdf + f'{i:0{2}}'+'.jpg'
                        if not os.path.exists(path):
                            break
                        spis.append({'path':path,'list':i})
                        if len(spis) == max_kol:
                            future = executor.submit(recognize_list, spis, tek_result)
                            futures.append(future)
                            spis = []
                        i = i+1
                    if len(spis) > 0:
                        future = executor.submit(recognize_list, spis, tek_result)
                        futures.append(future)
                    conc.wait(futures)
                    tek_result.sort(key=lambda x: x[0])
                    for el in tek_result:
                        if el[2]!='':
                            FileResult.append({'list': el[0], 'error':el[2]})
                        else:
                            FileResult.append({'list': el[0], 'result':el[1]})

                    result.append({'id_file': _file["id_file"], 'result': FileResult})
                else:
                    print('list ' + str(1) + ' ' + fname + ' ' + str(datetime.datetime.now()))
                    rec_file = recognzie(fname,r'C:\Program Files\Tesseract-OCR\tesseract.exe')
                    FileResult.append({'list': 1, 'result': rec_file})
                    result.append({'id_file': _file["id_file"],'result':FileResult})
            except:
                result.append({'id_file': _file["id_file"],'error': str(sys.exc_info()[1])})
            os.remove(fname)
        # Теперь нужно распределить результат
        return json.dumps(result, ensure_ascii=False), 201

    def put(self, id=0):
        return f"must use POST method", 201

    def delete(self, id=0):
        return f"must use POST method", 200

def recognize_list(spis, result):
    for el_spis in spis:
        path = el_spis['path']
        list = el_spis['list']
        print('list ' + str(list) + ' ' + path + ' ' + str(datetime.datetime.now()))
        try:
            rec_file = recognzie(path, r'C:\Program Files\Tesseract-OCR\tesseract.exe')
            result.append([list,rec_file,''])
        except:
            result.append([list, '', str(sys.exc_info()[1])])
        os.remove(path)

@app.route('/')
def hello():
    return platform

api.add_resource(Quote, "/recognaize", "/recognaize/", "/recognaize/<int:id>")
if __name__ == '__main__':
    app.run(host = '0.0.0.0',port='5000',debug=False)
    #app.run(debug=True)
