from enum import Enum


class AdministrationType(Enum):
    CENTRAL_ADMINISTRATION = "Administración Central"
    INTERSTATE_COMPANY = "Empresa Interestadual"
    STATE_OWNED_COMPANY = "Empresa del estado"
    INTERSTATE_ENTITY = "Ente Interestadual"
    BINATIONAL_ENTITY = "Ente binacional"
    NON_STATE_PUBLIC_ENTITY = "Ente público no estatal"
    TRUST_FUND = "Fondo Fiduciario"
    STATE_SOCIAL_SECURITY_PROVIDER = "Obra Social Estatal"
    DECENTRALIZED_AGENCY = "Organismo Descentralizado"
    DECONCENTRATED_AGENCY = "Organismo Desconcentrado"
    OFFICIAL_BANKING_SYSTEM = "Sistema bancario oficial"
    CORPORATION = "Sociedad Anónima"
    SINGLE_MEMBER_CORPORATION = "Sociedad Anónima Unipersonal"
    MAJORITY_STATE_OWNED_CORPORATION = "Sociedad Anónima con participación estatal mayoritaria"
    STATE_OWNED_ENTERPRISE = "Sociedad del Estado"
