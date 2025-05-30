from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
import akahu.models.transaction as models
from bagels.models.category import Nature
from bagels.models.database.app import Base


class AkahuCategory(Base):
    __tablename__ = "akahu_category"
    akahuId = Column(String, primary_key=True)
    akahuGroupId = Column(String, ForeignKey("akahu_group.akahuId"), nullable=True)
    categoryId = Column(Integer, ForeignKey("category.id"), nullable=True)
    createdAt = Column(DateTime, nullable=False, default=datetime.now())
    updatedAt = Column(DateTime, nullable=False, default=datetime.now())

    name = Column(String, nullable=False)
    nature = Column(SQLEnum(Nature), nullable=False)
    color = Column(String, nullable=False)

    group = relationship("AkahuGroup", foreign_keys=[akahuGroupId])
    category = relationship("Category", foreign_keys=[categoryId])

    @staticmethod
    def create_from_akahu(category: models.Category) -> "AkahuCategory":
        group = category.groups.personal_finance if category.groups else None
        return AkahuCategory(
            akahuId=category.id,
            akahuGroupId=group.id if group else None,
            name=category.name,
            color="#aa00aa",
            nature=Nature.WANT,
        )

    def update_from_akahu(self, category: models.Category) -> "AkahuCategory":
        changes = AkahuCategory.create_from_akahu(category)
        self.akahuGroupId = changes.akahuGroupId
        self.name = changes.name
        self.color = changes.color
        self.nature = changes.nature
        return self
