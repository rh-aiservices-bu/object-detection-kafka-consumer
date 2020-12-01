import json


def inference(d):
    print(f'Executing Inference:')
    for key, value in d.items():
        if (isinstance(value, str) and len(value) > 40):
            print(f'{key}: {value[:20]}..{value[-20:]}')
        else:
            print(f'{key}: {value}')

    d = [
        {
            'box': {
                'xMax': 0.4,
                'xMin': 0.2,
                'yMax': 0.4,
                'yMin': 0.2
            },
            'class': 1,
            'label': 'dog',
            'score': 0.99
        }, {
            'box': {
                'xMax': 0.8,
                'xMin': 0.6,
                'yMax': 0.8,
                'yMin': 0.6
            },
            'class': 2,
            'label': 'cat',
            'score': 0.98
        }
    ]

    return d
