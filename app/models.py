from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship

from .database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(100), nullable=False, unique=True, index=True)
    cosplay_nick = Column(String(100), nullable=True, unique=True, index=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    home_city = Column(String(255), nullable=True, index=True)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    options = relationship("UserOption", back_populates="user", cascade="all, delete-orphan")
    cards = relationship(
        "CosplanCard",
        back_populates="user",
        cascade="all, delete-orphan",
        foreign_keys="CosplanCard.user_id",
    )
    shared_cards_sent = relationship(
        "CosplanCard",
        cascade="all, delete-orphan",
        foreign_keys="CosplanCard.shared_from_user_id",
    )
    in_progress_cards = relationship("InProgressCard", back_populates="user", cascade="all, delete-orphan")
    festivals = relationship("Festival", back_populates="user", cascade="all, delete-orphan")
    project_search_posts = relationship("ProjectSearchPost", back_populates="user", cascade="all, delete-orphan")
    community_questions = relationship("CommunityQuestion", back_populates="user", cascade="all, delete-orphan")
    community_question_comments = relationship(
        "CommunityQuestionComment",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_masters = relationship("CommunityMaster", back_populates="user", cascade="all, delete-orphan")
    community_master_comments = relationship(
        "CommunityMasterComment",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_articles = relationship("CommunityArticle", back_populates="user", cascade="all, delete-orphan")
    community_article_comments = relationship(
        "CommunityArticleComment",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_article_favorites = relationship(
        "CommunityArticleFavorite",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    festival_announcements_requested = relationship(
        "FestivalAnnouncement",
        back_populates="requester",
        cascade="all, delete-orphan",
        foreign_keys="FestivalAnnouncement.requester_user_id",
    )
    festival_announcements_reviewed = relationship(
        "FestivalAnnouncement",
        back_populates="reviewer",
        foreign_keys="FestivalAnnouncement.reviewed_by_user_id",
    )
    incoming_notifications = relationship(
        "FestivalNotification",
        back_populates="recipient",
        cascade="all, delete-orphan",
        foreign_keys="FestivalNotification.user_id",
    )
    outgoing_notifications = relationship(
        "FestivalNotification",
        back_populates="sender",
        cascade="all, delete-orphan",
        foreign_keys="FestivalNotification.from_user_id",
    )
    card_comments = relationship("CardComment", back_populates="author", cascade="all, delete-orphan")
    rehearsal_cards = relationship("RehearsalCard", back_populates="user", cascade="all, delete-orphan")
    rehearsal_entries = relationship(
        "RehearsalEntry",
        back_populates="participant",
        cascade="all, delete-orphan",
        foreign_keys="RehearsalEntry.user_id",
    )
    rehearsal_entries_created = relationship(
        "RehearsalEntry",
        back_populates="proposer",
        foreign_keys="RehearsalEntry.proposed_by_user_id",
    )


class UserOption(Base):
    __tablename__ = "user_options"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    group = Column(String(64), nullable=False, index=True)
    value = Column(String(255), nullable=False)

    user = relationship("User", back_populates="options")

    __table_args__ = (UniqueConstraint("user_id", "group", "value", name="uq_user_option_value"),)


class CosplanCard(Base):
    __tablename__ = "cosplan_cards"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    character_name = Column(String(255), nullable=False, index=True)
    fandom = Column(String(255), nullable=True, index=True)
    is_au = Column(Boolean, nullable=False, default=False)
    au_text = Column(Text, nullable=True)

    costume_type = Column(String(32), nullable=True)  # sew | buy
    sewing_type = Column(String(32), nullable=True)  # self | outsourced
    sewing_fabric = Column(Boolean, nullable=False, default=False)
    sewing_hardware = Column(Boolean, nullable=False, default=False)
    sewing_pattern = Column(Boolean, nullable=False, default=False)
    costume_executor = Column(String(255), nullable=True)
    costume_deadline = Column(Date, nullable=True)
    costume_prepayment = Column(Float, nullable=True)
    costume_postpayment = Column(Float, nullable=True)
    costume_fabric_price = Column(Float, nullable=True)
    costume_hardware_price = Column(Float, nullable=True)
    costume_bought = Column(Boolean, nullable=False, default=False)
    costume_link = Column(Text, nullable=True)
    costume_buy_price = Column(Float, nullable=True)
    costume_currency = Column(String(16), nullable=True)
    costume_notes = Column(Text, nullable=True)

    shoes_type = Column(String(32), nullable=True)  # buy | craft
    shoes_bought = Column(Boolean, nullable=False, default=False)
    shoes_link = Column(Text, nullable=True)
    shoes_buy_price = Column(Float, nullable=True)
    shoes_executor = Column(String(255), nullable=True)
    shoes_deadline = Column(Date, nullable=True)
    shoes_price = Column(Float, nullable=True)
    shoes_currency = Column(String(16), nullable=True)

    lenses_enabled = Column(Boolean, nullable=False, default=False)
    lenses_comment = Column(Text, nullable=True)
    lenses_color = Column(String(64), nullable=True)
    lenses_price = Column(Float, nullable=True)
    lenses_currency = Column(String(16), nullable=True)

    wig_type = Column(String(32), nullable=True)  # wigmaker | buy | no_buy
    wigmaker_name = Column(String(255), nullable=True)
    wig_price = Column(Float, nullable=True)
    wig_buy_price = Column(Float, nullable=True)
    wig_currency = Column(String(16), nullable=True)
    wig_deadline = Column(Date, nullable=True)
    wig_link = Column(Text, nullable=True)
    wig_no_buy_from = Column(String(255), nullable=True)
    wig_restyle = Column(Boolean, nullable=False, default=False)

    craft_type = Column(String(32), nullable=True)  # self | order
    craft_master = Column(String(255), nullable=True)
    craft_price = Column(Float, nullable=True)
    craft_material_price = Column(Float, nullable=True)
    craft_deadline = Column(Date, nullable=True)
    craft_currency = Column(String(16), nullable=True)

    plan_type = Column(String(32), nullable=True)  # project | personal
    project_leader = Column(String(255), nullable=True)
    cosbands_json = Column(JSON, nullable=False, default=list)
    project_deadline = Column(Date, nullable=True)
    related_cards_json = Column(JSON, nullable=False, default=list)

    planned_festivals_json = Column(JSON, nullable=False, default=list)
    submission_date = Column(Date, nullable=True)
    nominations_json = Column(JSON, nullable=False, default=list)
    city = Column(String(255), nullable=True)

    photographers_json = Column(JSON, nullable=False, default=list)
    studios_json = Column(JSON, nullable=False, default=list)
    photoset_date = Column(Date, nullable=True)
    photoset_price = Column(Float, nullable=True)
    photoset_photographer_price = Column(Float, nullable=True)
    photoset_studio_price = Column(Float, nullable=True)
    photoset_props_price = Column(Float, nullable=True)
    photoset_extra_price = Column(Float, nullable=True)
    photoset_currency = Column(String(16), nullable=True)
    photoset_comment = Column(Text, nullable=True)
    photoset_props_checklist_json = Column(JSON, nullable=False, default=list)
    performance_track = Column(String(255), nullable=True)
    performance_video_bg_url = Column(Text, nullable=True)
    performance_script = Column(Text, nullable=True)
    performance_light_script = Column(Text, nullable=True)
    performance_duration = Column(String(8), nullable=True)
    performance_rehearsal_point = Column(String(255), nullable=True)
    performance_rehearsal_price = Column(Float, nullable=True)
    performance_rehearsal_currency = Column(String(16), nullable=True)
    performance_rehearsal_count = Column(Integer, nullable=True)
    references_json = Column(JSON, nullable=False, default=list)
    pose_references_json = Column(JSON, nullable=False, default=list)
    unknown_prices_json = Column(JSON, nullable=False, default=list)
    costume_parts_json = Column(JSON, nullable=False, default=list)
    craft_parts_json = Column(JSON, nullable=False, default=list)

    coproplayers_json = Column(JSON, nullable=False, default=list)
    coproplayer_nicks_json = Column(JSON, nullable=False, default=list)

    # Shared copy support: if this is a propagated card for another user.
    is_shared_copy = Column(Boolean, nullable=False, default=False)
    source_card_id = Column(Integer, ForeignKey("cosplan_cards.id", ondelete="SET NULL"), nullable=True, index=True)
    shared_from_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    notes = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="cards", foreign_keys=[user_id])
    in_progress = relationship(
        "InProgressCard",
        back_populates="cosplan_card",
        uselist=False,
        cascade="all, delete-orphan",
        single_parent=True,
    )
    source_card = relationship("CosplanCard", remote_side=[id], foreign_keys=[source_card_id])
    comments = relationship("CardComment", back_populates="card", cascade="all, delete-orphan")
    rehearsal_cards = relationship("RehearsalCard", back_populates="cosplan_card", cascade="all, delete-orphan")
    rehearsal_entries = relationship("RehearsalEntry", back_populates="cosplan_card", cascade="all, delete-orphan")


class CardComment(Base):
    __tablename__ = "card_comments"

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey("cosplan_cards.id", ondelete="CASCADE"), nullable=False, index=True)
    author_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    parent_id = Column(Integer, ForeignKey("card_comments.id", ondelete="SET NULL"), nullable=True, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    card = relationship("CosplanCard", back_populates="comments")
    author = relationship("User", back_populates="card_comments")
    parent = relationship("CardComment", remote_side=[id], back_populates="replies")
    replies = relationship("CardComment", back_populates="parent")


class InProgressCard(Base):
    __tablename__ = "in_progress_cards"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    cosplan_card_id = Column(Integer, ForeignKey("cosplan_cards.id", ondelete="CASCADE"), nullable=False, unique=True)
    checklist_json = Column(JSON, nullable=False, default=list)
    task_rows_json = Column(JSON, nullable=False, default=list)
    is_frozen = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="in_progress_cards")
    cosplan_card = relationship("CosplanCard", back_populates="in_progress")


class RehearsalCard(Base):
    __tablename__ = "rehearsal_cards"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    cosplan_card_id = Column(Integer, ForeignKey("cosplan_cards.id", ondelete="CASCADE"), nullable=False, index=True)
    deadline_date = Column(Date, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="rehearsal_cards")
    cosplan_card = relationship("CosplanCard", back_populates="rehearsal_cards")
    entries = relationship("RehearsalEntry", back_populates="rehearsal_card", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("user_id", "cosplan_card_id", name="uq_rehearsal_card_user_cosplan"),
    )


class RehearsalEntry(Base):
    __tablename__ = "rehearsal_entries"

    id = Column(Integer, primary_key=True)
    rehearsal_card_id = Column(Integer, ForeignKey("rehearsal_cards.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    cosplan_card_id = Column(Integer, ForeignKey("cosplan_cards.id", ondelete="CASCADE"), nullable=False, index=True)
    proposed_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    source_type = Column(String(32), nullable=False, index=True)  # participant | leader
    status = Column(String(32), nullable=False, index=True)  # proposed | approved | accepted | declined
    entry_date = Column(Date, nullable=False, index=True)
    entry_time = Column(String(8), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    rehearsal_card = relationship("RehearsalCard", back_populates="entries")
    participant = relationship("User", back_populates="rehearsal_entries", foreign_keys=[user_id])
    proposer = relationship("User", back_populates="rehearsal_entries_created", foreign_keys=[proposed_by_user_id])
    cosplan_card = relationship("CosplanCard", back_populates="rehearsal_entries")


class Festival(Base):
    __tablename__ = "festivals"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    name = Column(String(255), nullable=False, index=True)
    url = Column(Text, nullable=True)
    city = Column(String(255), nullable=True, index=True)
    event_date = Column(Date, nullable=True)
    event_end_date = Column(Date, nullable=True)
    submission_deadline = Column(Date, nullable=True)

    nomination_1 = Column(String(255), nullable=True)
    nomination_2 = Column(String(255), nullable=True)
    nomination_3 = Column(String(255), nullable=True)

    is_going = Column(Boolean, nullable=False, default=False)
    going_coproplayers_json = Column(JSON, nullable=False, default=list)
    is_global_announcement = Column(Boolean, nullable=False, default=False)
    source_announcement_id = Column(Integer, ForeignKey("festival_announcements.id", ondelete="SET NULL"), nullable=True, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="festivals")


class ProjectSearchPost(Base):
    __tablename__ = "project_search_posts"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    fandom = Column(String(255), nullable=False, index=True)
    event_date = Column(Date, nullable=True, index=True)
    event_type = Column(String(32), nullable=False, index=True)  # photoset | festival
    status = Column(String(32), nullable=False, default="active", index=True)  # active | found | inactive
    comment = Column(Text, nullable=True)
    contact_nick = Column(String(100), nullable=False)
    contact_link = Column(String(255), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="project_search_posts")


class FestivalNotification(Base):
    __tablename__ = "festival_notifications"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    from_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    source_card_id = Column(Integer, ForeignKey("cosplan_cards.id", ondelete="SET NULL"), nullable=True, index=True)
    message = Column(Text, nullable=False)
    is_read = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    recipient = relationship("User", back_populates="incoming_notifications", foreign_keys=[user_id])
    sender = relationship("User", back_populates="outgoing_notifications", foreign_keys=[from_user_id])


class FestivalAnnouncement(Base):
    __tablename__ = "festival_announcements"

    id = Column(Integer, primary_key=True)
    requester_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    reviewed_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    name = Column(String(255), nullable=False, index=True)
    url = Column(Text, nullable=True)
    city = Column(String(255), nullable=True, index=True)
    event_date = Column(Date, nullable=True)
    event_end_date = Column(Date, nullable=True)
    submission_deadline = Column(Date, nullable=True)
    nomination_1 = Column(String(255), nullable=True)
    nomination_2 = Column(String(255), nullable=True)
    nomination_3 = Column(String(255), nullable=True)

    status = Column(String(16), nullable=False, default="pending", index=True)  # pending | approved | rejected
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    requester = relationship("User", back_populates="festival_announcements_requested", foreign_keys=[requester_user_id])
    reviewer = relationship("User", back_populates="festival_announcements_reviewed", foreign_keys=[reviewed_by_user_id])


class CommunityArticle(Base):
    __tablename__ = "community_articles"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    topic = Column(String(255), nullable=False, index=True)
    author_name = Column(String(120), nullable=False)
    body_markdown = Column(Text, nullable=False)
    tags_json = Column(JSON, nullable=False, default=list)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_articles")
    comments = relationship("CommunityArticleComment", back_populates="article", cascade="all, delete-orphan")
    favorites = relationship("CommunityArticleFavorite", back_populates="article", cascade="all, delete-orphan")


class CommunityArticleComment(Base):
    __tablename__ = "community_article_comments"

    id = Column(Integer, primary_key=True)
    article_id = Column(Integer, ForeignKey("community_articles.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    article = relationship("CommunityArticle", back_populates="comments")
    user = relationship("User", back_populates="community_article_comments")


class CommunityArticleFavorite(Base):
    __tablename__ = "community_article_favorites"

    id = Column(Integer, primary_key=True)
    article_id = Column(Integer, ForeignKey("community_articles.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    article = relationship("CommunityArticle", back_populates="favorites")
    user = relationship("User", back_populates="community_article_favorites")

    __table_args__ = (
        UniqueConstraint("article_id", "user_id", name="uq_community_article_favorite_user"),
    )


class CommunityQuestion(Base):
    __tablename__ = "community_questions"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    title = Column(String(255), nullable=False, index=True)
    body = Column(Text, nullable=False)
    status = Column(String(32), nullable=False, default="open", index=True)  # open | resolved

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_questions")
    comments = relationship("CommunityQuestionComment", back_populates="question", cascade="all, delete-orphan")


class CommunityQuestionComment(Base):
    __tablename__ = "community_question_comments"

    id = Column(Integer, primary_key=True)
    question_id = Column(Integer, ForeignKey("community_questions.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    question = relationship("CommunityQuestion", back_populates="comments")
    user = relationship("User", back_populates="community_question_comments")


class CommunityMaster(Base):
    __tablename__ = "community_masters"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    nick = Column(String(100), nullable=False, index=True)
    master_type = Column(String(64), nullable=False, index=True)
    details = Column(Text, nullable=False)
    gallery_json = Column(JSON, nullable=False, default=list)
    price_list_json = Column(JSON, nullable=False, default=list)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_masters")
    comments = relationship("CommunityMasterComment", back_populates="master", cascade="all, delete-orphan")


class CommunityMasterComment(Base):
    __tablename__ = "community_master_comments"

    id = Column(Integer, primary_key=True)
    master_id = Column(Integer, ForeignKey("community_masters.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    master = relationship("CommunityMaster", back_populates="comments")
    user = relationship("User", back_populates="community_master_comments")
