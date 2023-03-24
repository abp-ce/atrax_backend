import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Operator(Base):
    __tablename__ = "operator"
    inn = sa.Column(sa.BigInteger, primary_key=True)
    name = sa.Column(sa.String(150), nullable=False)
    phones = relationship("Phone", back_populates="operator")


class Region(Base):
    __tablename__ = "region"
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(150), nullable=False)
    sub_name = sa.Column(sa.String(150), nullable=True)
    phones = relationship("Phone", back_populates="region")
    __table_args__ = (
        sa.UniqueConstraint("name", "sub_name", name="name_sub_name_uc"),
    )


class Phone(Base):
    __tablename__ = "phone"
    prefix = sa.Column(sa.Integer, nullable=False, default=0)
    start = sa.Column(sa.Integer, nullable=False, default=0)
    end = sa.Column(sa.Integer, nullable=False, default=0)
    operator_inn = sa.Column(sa.BigInteger, sa.ForeignKey(Operator.inn))
    region_id = sa.Column(sa.Integer, sa.ForeignKey(Region.id))
    operator = relationship("Operator", back_populates="phones")
    region = relationship("Region", back_populates="phones")
    __table_args__ = (
        sa.PrimaryKeyConstraint("prefix", "start", "end", name="phone_pk"),
    )
