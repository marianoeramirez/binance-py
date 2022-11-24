import json


class APIException(Exception):

    def __init__(self, response, status_code, text):
        self.code = 0
        try:
            json_res = json.loads(text)
        except ValueError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(response.text)
        else:
            self.code = json_res['code']
            self.message = json_res['msg']
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


class RequestException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'RequestException: %s' % self.message


class OrderException(Exception):

    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return 'OrderException(code=%s): %s' % (self.code, self.message)


class OrderMinAmountException(OrderException):

    def __init__(self, value):
        message = "Amount must be a multiple of %s" % value
        super().__init__(-1013, message)


class OrderMinPriceException(OrderException):

    def __init__(self, value):
        message = "Price must be at least %s" % value
        super().__init__(-1013, message)


class OrderMinTotalException(OrderException):

    def __init__(self, value):
        message = "Total must be at least %s" % value
        super().__init__(-1013, message)


class OrderUnknownSymbolException(OrderException):

    def __init__(self, value):
        message = "Unknown symbol %s" % value
        super().__init__(-1013, message)


class OrderInactiveSymbolException(OrderException):

    def __init__(self, value):
        message = "Attempting to trade an inactive symbol %s" % value
        super().__init__(-1013, message)


class WebsocketUnableToConnect(Exception):
    pass


class NotImplementedException(Exception):
    def __init__(self, value):
        message = f'Not implemented: {value}'
        super().__init__(message)


class UnknownDateFormat(Exception):
    ...
