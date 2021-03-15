from flask import Flask, request
from flask_restful import Api, Resource
import base64
import os
import tempfile
import json
from Recognize import recognzie
import subprocess
import sys

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
                    # Использование fitz даёт отвратительное качество конвертации
                    # doc = fitz.open(fname)
                    # for i in range(0,len(doc)):
                    #     page = doc.loadPage(i)
                    #     pix = page.getPixmap()
                    #     try:
                    #         fhandle_pdf, fname_pdf = tempfile.mkstemp(suffix='.jpg',
                    #                                       dir=tempfile.gettempdir())
                    #         os.close(fhandle_pdf)
                    #         pix.writeImage(fname_pdf)
                    #         rec_file = recognzie(fname_pdf, r'C:\Program Files\Tesseract-OCR\tesseract.exe')
                    #         result.append({'id': _file["id"], 'result': rec_file})
                    #     except:
                    #         result.append({'id': _file["id"], 'error': r"error recognize file"})
                    #     os.remove(fname_pdf)
                    # doc.close()
                    # использование ghostscript лучше, но непонятно что с лицензией на использование
                    fhandle_pdf, fname_pdf = tempfile.mkstemp(suffix='',dir=tempfile.gettempdir())
                    os.close(fhandle_pdf)
                    os.remove(fname_pdf)
                    out = fname_pdf+'%02d.jpg'
                    cmd = '"'+gs+'" -dBATCH -dNOPAUSE -sDEVICE=jpeg -r500 -dSAFER -sOutputFile="'+out+'" "'+fname+'"'
                    subprocess.check_output(cmd)
                    i = 1

                    while True:
                        path = fname_pdf + f'{i:0{2}}'+'.jpg'
                        if not os.path.exists(path):
                            break
                        try:
                            rec_file = recognzie(path, r'C:\Program Files\Tesseract-OCR\tesseract.exe')
                            FileResult.append({'list': i, 'result': rec_file})
                        except:
                            FileResult.append({'list': i, 'error': str(sys.exc_info()[1])})
                        os.remove(path)
                        i = i+1
                    result.append({'id_file': _file["id_file"], 'result': FileResult})

                else:
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

api.add_resource(Quote, "/recognaize", "/recognaize/", "/recognaize/<int:id>")
if __name__ == '__main__':
    app.run(host = '0.0.0.0',port=32029,debug=True)
    #app.run(debug=True)