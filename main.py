from etl import Pipline

if __name__ == '__main__':
    Pipline.bronze_layer()
    Pipline.silver_layer()
    Pipline.gold_layer()
