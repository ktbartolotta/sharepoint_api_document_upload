import sys
import pyodbc
import requests
import getpass
import traceback

from requests_ntlm import HttpNtlmAuth


def main():

    user_name = raw_input("username:")
    password = getpass.getpass("password:")
    auth = HttpNtlmAuth("PPL\\%s" % user_name, password)

    api_url = "http://engagesites.ppl.com/energysupply/gtspc/_api"

    """
    Extract X-RequestDigest value from site."""
    url = "/".join([api_url, "contextinfo"])
    headers = {
            "accept": "application/json;odata=verbose"
        }
    n = requests.post(
        url=url,
        auth=auth,
        headers=headers)

    xrd = n.json()['d']['GetContextWebInformation']['FormDigestValue']

    try:
        cnn_str = ";".join([
                "Driver={SQL Server}", "Server=WIN-DBFM-G-100\\INST05",
                "Trusted_Connection=yes", "database=Impact"
            ])
        cnn = pyodbc.connect(cnn_str)
        cur = cnn.cursor()
        query = """
                select FileName, Data
                from dbo.PPL_VW_TALEN_ATTACHMENTS
                where data is not null
                    and FileName like '%.xlsx%'"""
        cur.execute(query)
        for r in cur:
            try:
                url = "/".join([
                        api_url, "web",
                        "GetFolderByServerRelativeUrl('%s')",
                        "files", "add(url='%s',overwrite=true)"
                    ]) % ("ar_docs_test", r[0])
                headers = {
                        "accept": "application/json;odata=verbose",
                        "content-type":
                        "application/x-www-urlencoded; charset=UTF-8",
                        "content-length": len(r[1]),
                        "X-RequestDigest": xrd
                    }
                t = requests.post(
                    url=url,
                    auth=auth,
                    headers=headers,
                    data=r[1])
                print(t.status_code)
            except:
                traceback.print_exc(file=sys.stdout)
        cur.close()
        cnn.close()
    except:
        traceback.print_exc(file=sys.stdout)


if __name__ == '__main__':

    main()
