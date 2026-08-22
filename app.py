from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def homes():
    return render_template('home.html')

# Fixed the template name to match the route purpose
@app.route('/sig')
def signup():
    return render_template('signin.html')

if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')
