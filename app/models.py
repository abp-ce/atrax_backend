import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import INT8RANGE, ExcludeConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Operator(Base):
    __tablename__ = "operator"
    id = sa.Column(sa.Integer, primary_key=True)
    inn = sa.Column(sa.BigInteger, nullable=True)
    name = sa.Column(sa.String(150), nullable=False)
    phones = relationship("Phone", back_populates="operator")
    __table_args__ = (sa.UniqueConstraint("inn", "name", name="inn_mame_uc"),)


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
    id = sa.Column(sa.Integer, primary_key=True)
    range = sa.Column(INT8RANGE, nullable=False)
    # prefix = sa.Column(sa.Integer, nullable=False, default=0)
    # start = sa.Column(sa.Integer, nullable=False, default=0)
    # end = sa.Column(sa.Integer, nullable=False, default=0)
    operator_id = sa.Column(sa.BigInteger, sa.ForeignKey(Operator.id))
    region_id = sa.Column(sa.Integer, sa.ForeignKey(Region.id))
    operator = relationship("Operator", back_populates="phones")
    region = relationship("Region", back_populates="phones")
    __table_args__ = (
        ExcludeConstraint(
            ("range", "&&"),
        ),
        # sa.PrimaryKeyConstraint("prefix", "start", "end", name="phone_pk"),
    )
