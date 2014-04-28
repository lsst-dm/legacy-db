try:
    import pkgutil, lsstimport
    __path__ = pkgutil.extend_path(__path__, __name__)
except:
    pass
