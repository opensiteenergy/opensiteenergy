import logging
from opensite.app.opensite import lifespan, OpenSiteApplication

opensiteenergy = OpenSiteApplication(logging.INFO)
app = opensiteenergy.app
app.router.lifespan_context = lifespan

def main():
    # Run OpenSite application
    opensiteenergy.setup()
    opensiteenergy.run()

if __name__ == "__main__":
    main()