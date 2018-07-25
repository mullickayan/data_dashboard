from flask import Flask, render_template, request
from werkzeug import secure_filename
import os
import time
#Custom modules
import dashboard_parameters
from data_ingestion import di_controller
from utility import log_writer

#Initialize variables
upload_folder = str(dashboard_parameters.upload_folder)
upload_html = str(dashboard_parameters.upload_html)
output_html = str(dashboard_parameters.output_html)
data_ingestion_log_nm = str(dashboard_parameters.data_ingestion_log_nm)
data_ingestion_log_ext = str(dashboard_parameters.data_ingestion_log_ext)

#Initialize app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = upload_folder

#URL rule for upload
@app.route('/upload')
def upload_file():
   return render_template(upload_html)

#URL rule for output	
@app.route('/result', methods = ['POST'])
def uploader():
   if request.method == 'POST':
      f = request.files['file']
      uploaded_file = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(f.filename))
      f.save(uploaded_file)
      
      if 'DI'=='DI': #This will come from request object of home screen
          log_file = data_ingestion_log_nm + str(time.strftime("%d-%m-%y-%H-%M-%S")) + data_ingestion_log_ext
          logger = log_writer.run(log_file, 'DI')
          logger.info('Starting data ingestion')
          output = di_controller.run(uploaded_file)
                  
          #return output
          return render_template(output_html, result=output)
		
if __name__ == '__main__':
   app.run(debug = True)   
   