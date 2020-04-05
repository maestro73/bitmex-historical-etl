import math


def calc_exponent(volume, divisor=10, decimal_places=1):
    is_round = volume % math.pow(divisor, decimal_places) == 0
    if is_round:
        decimal_places += 1
        stop_execution = False
        while not stop_execution:
            is_round = volume % math.pow(divisor, decimal_places) == 0
            if is_round:
                decimal_places += 1
            else:
                stop_execution = True
        return decimal_places - 1
    else:
        return 0


def calculate_exponent(data_frame):
    data_frame["exponent"] = data_frame.volume.apply(calc_exponent)
    return data_frame
