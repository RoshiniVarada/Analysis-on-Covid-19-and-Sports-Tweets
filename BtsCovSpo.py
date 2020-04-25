from flask import Flask, send_file  # import flask

from Q1 import plot1, plot1
from Q2 import plot2, plot2
from Q3 import plot3, plot3
from Q4 import plot4, plot4
from Q5 import plot5, plot5
from Q6 import plot6, plot6
from Q7 import plot7, plot7
from Q8 import plot8, plot8
from Q9 import plot9, plot9
from Q10 import plot10, plot10
from Q11 import plot11, plot11
from Q12 import plot12, plot12
from Q13 import plot13,plot13
from Q14 import plot14,plot14



app = Flask(__name__) # create an app instance

@app.route("/")                   # at the end point /
def hello():                      # call method hello
    return "Hello World!"         # which returns "hello world

@app.route('/plots/Query1', methods=['GET'])
def matrix1():
   bytes_obj = plot1()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query2', methods=['GET'])
def matrix2():
   bytes_obj = plot2()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query3', methods=['GET'])
def matrix3():
   bytes_obj = plot3()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query4', methods=['GET'])
def matrix4():
   bytes_obj = plot4()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query5', methods=['GET'])
def matrix5():
   bytes_obj = plot5()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query6', methods=['GET'])
def matrix6():
   bytes_obj = plot6()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query7', methods=['GET'])
def matrix7():
   bytes_obj = plot7()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query8', methods=['GET'])
def matrix8():
   bytes_obj = plot8()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query9', methods=['GET'])
def matrix9():
   bytes_obj = plot9()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query10', methods=['GET'])
def matrix10():
   bytes_obj = plot10()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query11', methods=['GET'])
def matrix11():
   bytes_obj = plot11()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query12', methods=['GET'])
def matrix12():
   bytes_obj = plot12()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query13', methods=['GET'])
def matrix13():
   bytes_obj = plot13()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')

@app.route('/plots/Query14', methods=['GET'])
def matrix14():
   bytes_obj = plot14()

   return send_file(bytes_obj,
                attachment_filename='plot.png',
                mimetype='image/png')



if __name__== "__main__":        # on running python app.py
    app.run()