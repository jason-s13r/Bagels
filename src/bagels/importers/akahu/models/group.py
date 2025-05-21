from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
import akahu.models.transaction as models
from bagels.models.category import Nature
from bagels.models.database.app import Base


class AkahuGroup(Base):
    __tablename__ = "akahu_group"
    akahuId = Column(String, primary_key=True)
    groupId = Column(Integer, ForeignKey("category.id"), nullable=True)
    createdAt = Column(DateTime, nullable=False, default=datetime.now())
    updatedAt = Column(DateTime, nullable=False, default=datetime.now())

    name = Column(String, nullable=False)
    nature = Column(SQLEnum(Nature), nullable=False)
    color = Column(String, nullable=False)

    group = relationship("Category", foreign_keys=[groupId])

    @staticmethod
    def create_from_akahu(group: models.Group) -> "AkahuGroup":
        return AkahuGroup(
            akahuId=group.id,
            name=group.name,
            color="#808080",
            nature=Nature.WANT,
        )

    def update_from_akahu(self, group: models.Group) -> "AkahuGroup":
        changes = AkahuGroup.create_from_akahu(group)
        self.name = changes.name
        self.color = changes.color
        self.nature = changes.nature
        return self
