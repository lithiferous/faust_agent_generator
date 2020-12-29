from __future__ import print_function

try:
    from ruamel import yaml
except ImportError:
    import yaml


# Map a Docker Compose "service" name to its image.
SERVICES = {
    'client': 'ubuntu:14.04',
    '8u121-jre-alpine': 'openjdk:8u121-jre-alpine',
    '8u121-jre': 'openjdk:8u121-jre',
}


# docker-compose.yml skeleton to fill out using "service" entries above.
COMPOSITION = {'services': {}, 'version': '3'}


def servicize(name, image):
    '''Create a docker-compose.yml service-section entry.
    Params
    ======
    name : str
        Name of the service.
    image : str
        Docker image to use (what your FROM line would contain).
    Returns
    =======
        dict describing the service. A name starting with 'client' is special.
    '''
    entry = {'command': ['/entrypoint.sh'],
             'image': image,
             'volumes': ['./entrypoint.sh:/entrypoint.sh:ro']}
    if not name.startswith('client'):
        entry['ports'] = ['9000', '9443']
        entry['healthcheck'] = {
            'test': 'curl --fail --insecure https://localhost:9443/'}
        entry['volumes'].append('./svc:/svc:ro')
    return entry


if __name__ == '__main__':
    for name, image in SERVICES.items():
        COMPOSITION['services'][name] = servicize(name, image)
    print(yaml.dump(COMPOSITION, default_flow_style=False, indent=4), end='')
