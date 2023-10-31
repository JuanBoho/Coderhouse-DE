Coderhouse  
Data Engineering flex - #56005

**v.1.0** Extración de datos y creación de tablas.  
# Calidad del Aire en Latinoamérica

El proyecto crea un pipeline que extrae periódicamente datos relacionados con la calidad del aire en las principales ciudades de latinoamérica. Los datos son provistos por [OpenAq](https://openaq.org/) y en principio se centra en la información provista por sensores ubicados en Colombia, Argentina, Chile, Ecuador, Bolivia, Perú y Brasil.

Diariamente se extraerán datos de estos sensores para ser alamacenados posteriormente en un _DataWarehouse_ en dos tablas _locations_ y _aq_measurements_.

