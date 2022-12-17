

class Quote:

    def __init__(self,price,size):
        self.price = price
        self.size = size

    def __eq__(self, other):
        return self.price == other.price and self.size == other.size 

    def __str__(self):
        return f"{self.price}, {self.size}"