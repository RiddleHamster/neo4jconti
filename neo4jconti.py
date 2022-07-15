#!/usr/bin/python3
import ipaddress
import os
import pathlib
import re
from os.path import isfile
import validate_email
import neo4j
from tika import parser
import sys
import hashlib
from tqdm import tqdm
import coinaddrvalidator

# Defined below as per: https://neo4j.com/docs/api/python-driver/current/
NEO4J_SCHEMA = ""
NEO4J_HOST_NAME = ""
NEO4J_PORT = ""
NEO4J_URL = "{scheme}://{host_name}:{port}".format(scheme=NEO4J_SCHEMA, host_name=NEO4J_HOST_NAME, port=NEO4J_PORT)
NEO4J_USER = ""
NEO4J_PASSWORD = ""

# Defined below as per: https://github.com/chrismattmann/tika-python/blob/master/tika/tika.py
TIKA_PORT = ""
TIKA_HOST = ""
TIKA_SERVER_ENDPOINT = "http://{host_name}:{port}".format(host_name=TIKA_HOST, port=TIKA_PORT)


def extract_btc(text):
    """Regex for btc addresses"""
    extracted_btc = []
    for match in re.finditer(r'\b(bc1|[13])[a-zA-HJ-NP-Z0-9]{25,39}\b', text):
        btc = match.group()
        btc_validation = coinaddrvalidator.validate('btc', btc)
        if btc_validation.valid:
            extracted_btc.append(btc)
    return extracted_btc


def extract_email(text):
    """Regex for email addresses"""
    extracted_email = []
    for match in re.finditer(
            r'''\b\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*\b''',
            text):
        email = match.group()
        if validate_email.validate_email(email):
            extracted_email.append(email)
    return extracted_email


def extract_ipv4(text):
    """Regex for ipv4 addresses"""
    extracted_ipv4 = []
    for match in re.finditer(
            r'\b(25[0-5]|\b2[0-4][0-9]|\b[01]?[0-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}\b',
            text):
        ipv4 = match.group()
        if validate_ip_address(ipv4):
            extracted_ipv4.append(ipv4)
    return extracted_ipv4


def validate_ip_address(address):
    try:
        ipaddress.ip_address(address)
        return True
    except ValueError:
        return False


def data_extract(text):
    """Builder function for extractions"""
    extracted_data = {}

    # print("Extracting email")
    email = extract_email(text)

    # print("Extracting ipv4")
    ipv4 = extract_ipv4(text)

    # print("Extracting btc")
    btc = extract_btc(text)

    extracted_data['email'] = []
    extracted_data['ipv4'] = []
    extracted_data['btc'] = []

    extracted_data['email'] = list(set(email))
    extracted_data['ipv4'] = list(set(ipv4))
    extracted_data['btc'] = list(set(btc))

    return extracted_data


def calculate_hash(file):
    """Function to find MD5 hash value of a file, based on:
    https://www.quickprogrammingtips.com/python/how-to-calculate-md5-hash-of-a-file-in-python.html """

    md5_hash = hashlib.md5()
    with tqdm(total=os.path.getsize(file), leave=False) as pbar:
        with open(file, "rb") as f:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
                pbar.update(len(byte_block))
            md5sum = md5_hash.hexdigest()
    return md5sum


def generate_neo4j_properties(extracted_data):
    """Takes Key Value pair and generates formatted Neo4j properties string where Key: [Value1,Value2]"""
    properties = ""
    for key in extracted_data:
        if len(extracted_data[key]) != 0 and str(extracted_data[key][0]).strip() != "":
            properties += key + ": '" + str(extracted_data[key]).replace("'", "") + "', "
    return properties[:-2]


def nodes_create(extracted_data, filename, filemd5sum):
    """Creation of Neo4j nodes and relationships - this is the main worker function for this project"""

    properties = generate_neo4j_properties(extracted_data)

    for key in extracted_data:
        for value in extracted_data[key]:
            node_1_file_hash_value = "MERGE (file: Filename {Filename:'" + filename + "',MD5: '" + filemd5sum + "'," + properties + "})"
            node_2_key_value = "MERGE (" + key + ":" + key + "{" + key + ":'" + value + "'})"
            relationship = "MATCH (file:Filename {" + "MD5: '" + filemd5sum + "'})" + ", (" + key + ":" + key + "{" + key + ":'" + value + "'})" + " MERGE (file)-[:" + key + "]-(" + key + ")"
            driver = neo4j.GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWORD))
            driver.session().run(node_1_file_hash_value)
            driver.session().run(node_2_key_value)
            driver.session().run(relationship)
            driver.session().close()


def generate_output_file(outputfilename):
    outfile = open(outputfilename, 'w+')
    header = "source" + "\t" + "md5" + "\t" + "type" + "\t" + "value"
    outfile.write("%s\n" % header)


def generate_output(inputfile, inputfilemd5sum, extracted_data, outputfilename):
    outfile = open(outputfilename, 'a+')

    for key in extracted_data:
        for value in extracted_data[key]:
            data = inputfile + "\t" + inputfilemd5sum + "\t" + key + "\t" + value
            outfile.write("%s\n" % data)


if __name__ == '__main__':

    inputfoldername = sys.argv[1]
    outputfilename = sys.argv[2]

    files = []

    generate_output_file(outputfilename)

    for file_path in pathlib.Path(inputfoldername).rglob("*.*"):
        if isfile(file_path):
            files.append(file_path.as_posix())

    for inputfilename in files:
        # Calculate MD5 of file
        print("Calculating MD5 of: ", inputfilename)
        filemd5sum = calculate_hash(inputfilename)

        print("Parsing: ", inputfilename)
        parsed_pdf = parser.from_file(inputfilename, TIKA_SERVER_ENDPOINT)
        data = str(parsed_pdf['content'])

        print("Extracting data from: ", inputfilename)
        # Extract data from file content
        extracted_data = data_extract(data)

        generate_output(inputfilename, filemd5sum, extracted_data, outputfilename)

        print("Creating nodes for: ", inputfilename)
        # Create Neo4j nodes and relationships
        nodes_create(extracted_data, inputfilename, filemd5sum)
