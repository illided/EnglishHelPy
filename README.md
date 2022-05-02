# EnglishHelPy
Small tool written in Python and HTML for learning english.

## Create database
HelPy comes with a prebuilt database of 400 videos. If you want to build
your own (with more videos for examples), run:
```
python3 db_filler/main.py
```
You can also try to build small test db (5 videos) with
```
python3 db_filler/main.py --is_test True
```

## Run application

HelPy already have [hosted demo](http://illided.pythonanywhere.com/), but you can run it locally.

Firstly install all necessary tools:
```
pip install -r requirements.txt
python3 -m spacy download en_core_web_sm
```

And then run the application:
```
python3 app/app.py
```

## Usage
HelPy takes a single English word and returns it's meaning and usage examples. 

![](https://i.ibb.co/41VwWSw/Screenshot-from-2022-05-02-15-55-14.png)
