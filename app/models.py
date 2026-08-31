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
    birth_date = Column(Date, nullable=True, index=True)
    telegram_chat_id = Column(String(64), nullable=True, unique=True, index=True)
    telegram_username = Column(String(255), nullable=True)
    telegram_linked_at = Column(DateTime(timezone=True), nullable=True)
    telegram_secret_code_hash = Column(String(255), nullable=True)
    telegram_secret_code_updated_at = Column(DateTime(timezone=True), nullable=True)
    vk_bot_user_id = Column(String(64), nullable=True, unique=True, index=True)
    vk_bot_peer_id = Column(String(64), nullable=True, unique=True, index=True)
    vk_bot_linked_at = Column(DateTime(timezone=True), nullable=True)
    vk_user_id = Column(String(64), nullable=True, unique=True, index=True)
    vk_screen_name = Column(String(255), nullable=True)
    yandex_user_id = Column(String(64), nullable=True, unique=True, index=True)
    avatar_path = Column(String(255), nullable=True)
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
    in_progress_master_cards = relationship(
        "InProgressMasterCard",
        back_populates="user",
        cascade="all, delete-orphan",
        foreign_keys="InProgressMasterCard.user_id",
    )
    in_progress_master_customer_cards = relationship(
        "InProgressMasterCard",
        back_populates="customer_user",
        foreign_keys="InProgressMasterCard.customer_user_id",
    )
    in_progress_master_comments = relationship(
        "InProgressMasterComment",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    festivals = relationship("Festival", back_populates="user", cascade="all, delete-orphan")
    event_management_events = relationship(
        "EventManagementEvent",
        back_populates="creator",
        cascade="all, delete-orphan",
        foreign_keys="EventManagementEvent.creator_user_id",
    )
    project_search_posts = relationship("ProjectSearchPost", back_populates="user", cascade="all, delete-orphan")
    project_search_comments = relationship(
        "ProjectSearchComment",
        back_populates="user",
        cascade="all, delete-orphan",
    )
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
    community_master_orders = relationship(
        "CommunityMasterOrder",
        back_populates="customer",
        cascade="all, delete-orphan",
    )
    community_master_search_posts = relationship(
        "CommunityMasterSearchPost",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_master_search_comments = relationship(
        "CommunityMasterSearchComment",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    character_library_entries = relationship(
        "CharacterLibraryEntry",
        back_populates="created_by",
        foreign_keys="CharacterLibraryEntry.created_by_user_id",
    )
    community_master_ratings = relationship(
        "CommunityMasterRating",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_cosplayers = relationship(
        "CommunityCosplayer",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_cosplayer_comments = relationship(
        "CommunityCosplayerComment",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_material_places = relationship(
        "CommunityMaterialPlace",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_material_place_comments = relationship(
        "CommunityMaterialPlaceComment",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_studios = relationship("CommunityStudio", back_populates="user", cascade="all, delete-orphan")
    community_studio_comments = relationship(
        "CommunityStudioComment",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_marketplace_sales = relationship(
        "CommunityMarketplaceSale",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    community_marketplace_searches = relationship(
        "CommunityMarketplaceSearch",
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
    personal_calendar_events = relationship(
        "PersonalCalendarEvent",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    work_shift_days = relationship(
        "WorkShiftDay",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    content_plan_posts = relationship(
        "ContentPlanPost",
        back_populates="user",
        cascade="all, delete-orphan",
        foreign_keys="ContentPlanPost.user_id",
    )
    content_channel_posts = relationship(
        "ContentChannelPost",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    title_entries = relationship(
        "TitleEntry",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    password_reset_tokens = relationship(
        "PasswordResetToken",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    home_news_entries = relationship(
        "HomeNews",
        back_populates="author",
    )
    photo_contest_requests = relationship(
        "PhotoContestRequest",
        back_populates="requester",
        cascade="all, delete-orphan",
        foreign_keys="PhotoContestRequest.requester_user_id",
    )
    photo_contest_requests_reviewed = relationship(
        "PhotoContestRequest",
        back_populates="reviewer",
        foreign_keys="PhotoContestRequest.reviewed_by_user_id",
    )
    photo_contests = relationship(
        "PhotoContest",
        back_populates="creator",
        cascade="all, delete-orphan",
        foreign_keys="PhotoContest.creator_user_id",
    )
    photo_contest_entries = relationship(
        "PhotoContestEntry",
        back_populates="participant",
        cascade="all, delete-orphan",
    )
    photo_contest_votes = relationship(
        "PhotoContestVote",
        back_populates="voter",
        cascade="all, delete-orphan",
    )


class UserOption(Base):
    __tablename__ = "user_options"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    group = Column(String(64), nullable=False, index=True)
    value = Column(String(255), nullable=False)

    user = relationship("User", back_populates="options")

    __table_args__ = (UniqueConstraint("user_id", "group", "value", name="uq_user_option_value"),)


class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    token_hash = Column(String(64), nullable=False, unique=True, index=True)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    used_at = Column(DateTime(timezone=True), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    user = relationship("User", back_populates="password_reset_tokens")


class PendingRegistration(Base):
    __tablename__ = "pending_registrations"

    id = Column(Integer, primary_key=True)
    username = Column(String(100), nullable=False, index=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password_hash = Column(String(255), nullable=False)
    telegram_secret_code_hash = Column(String(255), nullable=True)
    is_smm_manager = Column(Boolean, nullable=False, default=False)
    code_hash = Column(String(64), nullable=False, index=True)
    attempts = Column(Integer, nullable=False, default=0)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class HomeNews(Base):
    __tablename__ = "home_news"

    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    author = relationship("User", back_populates="home_news_entries")


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
    sewing_mockup = Column(Boolean, nullable=False, default=False)
    sewing_fitting = Column(Boolean, nullable=False, default=False)
    sewing_details = Column(Boolean, nullable=False, default=False)
    costume_executor = Column(String(255), nullable=True)
    costume_deadline = Column(Date, nullable=True)
    costume_prepayment = Column(Float, nullable=True)
    costume_postpayment = Column(Float, nullable=True)
    costume_fabric_price = Column(Float, nullable=True)
    costume_hardware_price = Column(Float, nullable=True)
    costume_fabric_rows_json = Column(JSON, nullable=False, default=list)
    costume_hardware_rows_json = Column(JSON, nullable=False, default=list)
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
    status_percent = Column(Integer, nullable=False, default=0)

    plan_type = Column(String(32), nullable=True)  # project | personal
    project_leader = Column(String(255), nullable=True)
    cosbands_json = Column(JSON, nullable=False, default=list)
    project_deadline = Column(Date, nullable=True)
    related_cards_json = Column(JSON, nullable=False, default=list)
    project_characters_json = Column(JSON, nullable=False, default=list)

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
    photoset_storyboard_rows_json = Column(JSON, nullable=False, default=list)
    performance_track = Column(String(255), nullable=True)
    performance_video_bg_url = Column(Text, nullable=True)
    performance_script = Column(Text, nullable=True)
    performance_light_script = Column(Text, nullable=True)
    performance_duration = Column(String(8), nullable=True)
    performance_plan_json = Column(JSON, nullable=False, default=dict)
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
    is_priority = Column(Boolean, nullable=False, default=False)
    is_completed = Column(Boolean, nullable=False, default=False)
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
    linked_title_entries = relationship("TitleEntry", back_populates="linked_card")


class CharacterLibraryEntry(Base):
    __tablename__ = "character_library_entries"

    id = Column(Integer, primary_key=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    first_name = Column(String(255), nullable=False, index=True)
    last_name = Column(String(255), nullable=True)
    full_name_en = Column(String(255), nullable=True)
    full_name_original = Column(String(255), nullable=True)
    fandom = Column(String(255), nullable=True, index=True)
    fandom_en = Column(String(255), nullable=True, index=True)
    gender = Column(String(32), nullable=False, default="unspecified", index=True)
    height_cm = Column(Integer, nullable=True)
    skin_color = Column(String(120), nullable=True)
    eye_color = Column(String(64), nullable=True, index=True)
    hair_color = Column(String(120), nullable=True)
    hair_length = Column(String(120), nullable=True)
    apparent_age = Column(Integer, nullable=True)
    age = Column(Integer, nullable=True)
    references_json = Column(JSON, nullable=False, default=list)
    biography = Column(Text, nullable=True)
    extra_info = Column(Text, nullable=True)
    appearance_features = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    created_by = relationship("User", back_populates="character_library_entries")


class TitleEntry(Base):
    __tablename__ = "title_entries"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    linked_card_id = Column(Integer, ForeignKey("cosplan_cards.id", ondelete="SET NULL"), nullable=True, index=True)

    entry_kind = Column(String(16), nullable=False, index=True)  # watch | read
    title = Column(String(255), nullable=False, index=True)
    status = Column(String(32), nullable=False, default="plan", index=True)  # plan | in_progress | done
    source_url = Column(Text, nullable=True)
    deadline_date = Column(Date, nullable=True, index=True)

    watch_country = Column(String(120), nullable=True)
    watch_episode_count = Column(Integer, nullable=True)
    watch_release_type = Column(String(32), nullable=True)  # completed | ongoing | no_translation
    watch_current_episode = Column(Integer, nullable=True)

    read_publisher = Column(String(255), nullable=True)
    read_page_count = Column(Integer, nullable=True)
    read_chapter_count = Column(Integer, nullable=True)
    read_types_json = Column(JSON, nullable=False, default=list)
    read_genre = Column(String(255), nullable=True)
    read_current_page = Column(Integer, nullable=True)
    read_current_chapter = Column(Integer, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="title_entries")
    linked_card = relationship("CosplanCard", back_populates="linked_title_entries")


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


class InProgressMasterCard(Base):
    __tablename__ = "in_progress_master_cards"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    work_type = Column(String(32), nullable=False, default="other")
    name = Column(String(255), nullable=False, index=True)
    title_text = Column(String(255), nullable=True)
    customer_name = Column(String(255), nullable=True)
    customer_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    task_rows_json = Column(JSON, nullable=False, default=list)
    materials_json = Column(JSON, nullable=False, default=list)
    note = Column(Text, nullable=True)
    measurements_json = Column(JSON, nullable=False, default=list)
    references_json = Column(JSON, nullable=False, default=list)
    intermediate_deadlines_json = Column(JSON, nullable=False, default=list)
    deadline_date = Column(Date, nullable=True)
    cloud_url = Column(Text, nullable=True)
    status_percent = Column(Integer, nullable=False, default=0)
    is_archived = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="in_progress_master_cards", foreign_keys=[user_id])
    customer_user = relationship("User", back_populates="in_progress_master_customer_cards", foreign_keys=[customer_user_id])
    comments = relationship("InProgressMasterComment", back_populates="card", cascade="all, delete-orphan")
    board = relationship(
        "InProgressMasterBoard",
        back_populates="card",
        uselist=False,
        cascade="all, delete-orphan",
        single_parent=True,
    )


class InProgressMasterComment(Base):
    __tablename__ = "in_progress_master_comments"

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey("in_progress_master_cards.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    card = relationship("InProgressMasterCard", back_populates="comments")
    user = relationship("User", back_populates="in_progress_master_comments")


class InProgressMasterBoard(Base):
    __tablename__ = "in_progress_master_boards"

    id = Column(Integer, primary_key=True)
    card_id = Column(
        Integer,
        ForeignKey("in_progress_master_cards.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    state_json = Column(JSON, nullable=False, default=dict)
    updated_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    card = relationship("InProgressMasterCard", back_populates="board")
    updated_by = relationship("User", foreign_keys=[updated_by_user_id])


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


class PersonalCalendarEvent(Base):
    __tablename__ = "personal_calendar_events"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    event_date = Column(Date, nullable=False, index=True)
    event_time = Column(String(8), nullable=True)
    title = Column(String(255), nullable=False)
    event_city = Column(String(255), nullable=True, index=True)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="personal_calendar_events")


class WorkShiftDay(Base):
    __tablename__ = "work_shift_days"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    shift_date = Column(Date, nullable=False, index=True)
    is_half_day = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    user = relationship("User", back_populates="work_shift_days")

    __table_args__ = (
        UniqueConstraint("user_id", "shift_date", name="uq_work_shift_day_user_date"),
    )


class ContentPlanPost(Base):
    __tablename__ = "content_plan_posts"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    shared_pair_id = Column(String(64), nullable=True, index=True)
    shared_partner_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    is_repost = Column(Boolean, nullable=False, default=False)
    manual_publish_only = Column(Boolean, nullable=False, default=False)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    publish_date = Column(Date, nullable=False, index=True)
    publish_time = Column(String(8), nullable=True)
    socials_json = Column(JSON, nullable=False, default=list)
    rubric = Column(String(120), nullable=False, index=True)
    rubric_tag = Column(String(120), nullable=True)
    status = Column(String(32), nullable=False, default="plan", index=True)
    telegram_body_html = Column(Text, nullable=True)
    telegram_photos_json = Column(JSON, nullable=False, default=list)
    telegram_channels_json = Column(JSON, nullable=False, default=list)
    telegram_cleanup_photos_json = Column(JSON, nullable=False, default=list)
    telegram_message_id = Column(String(64), nullable=True)
    telegram_message_ids_json = Column(JSON, nullable=False, default=list)
    telegram_published_at = Column(DateTime(timezone=True), nullable=True)
    vk_groups_json = Column(JSON, nullable=False, default=list)
    vk_post_ids_json = Column(JSON, nullable=False, default=list)
    vk_published_at = Column(DateTime(timezone=True), nullable=True)
    pinterest_boards_json = Column(JSON, nullable=False, default=list)
    pinterest_pin_ids_json = Column(JSON, nullable=False, default=list)
    pinterest_published_at = Column(DateTime(timezone=True), nullable=True)
    threads_post_ids_json = Column(JSON, nullable=False, default=list)
    threads_published_at = Column(DateTime(timezone=True), nullable=True)
    rednote_published_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="content_plan_posts", foreign_keys=[user_id])


class ContentChannelPost(Base):
    __tablename__ = "content_channel_posts"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    platform = Column(String(32), nullable=False, default="telegram", index=True)
    chat_id = Column(String(64), nullable=False, index=True)
    chat_title = Column(String(255), nullable=True)
    message_id = Column(String(64), nullable=False, index=True)
    message_text = Column(Text, nullable=True)
    published_at = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="content_channel_posts")

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "platform",
            "chat_id",
            "message_id",
            name="uq_content_channel_post_unique",
        ),
    )


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
    nominations_json = Column(JSON, nullable=False, default=list)
    planned_nominations_json = Column(JSON, nullable=False, default=list)
    has_photo_cosplay = Column(Boolean, nullable=False, default=False)
    is_partner_festival = Column(Boolean, nullable=False, default=False)
    shared_note = Column(Text, nullable=True)
    icon_path = Column(String(255), nullable=True)

    is_going = Column(Boolean, nullable=False, default=False)
    going_coproplayers_json = Column(JSON, nullable=False, default=list)
    packlist_json = Column(JSON, nullable=False, default=list)
    tickets_required = Column(Boolean, nullable=False, default=False)
    ticket_outbound_json = Column(JSON, nullable=False, default=dict)
    ticket_return_json = Column(JSON, nullable=False, default=dict)
    ticket_files_json = Column(JSON, nullable=False, default=list)
    timing_event_start_date = Column(Date, nullable=True)
    timing_event_start_time = Column(String(8), nullable=True)
    timing_block_start_time = Column(String(8), nullable=True)
    is_global_announcement = Column(Boolean, nullable=False, default=False)
    source_announcement_id = Column(Integer, ForeignKey("festival_announcements.id", ondelete="SET NULL"), nullable=True, index=True)
    import_source = Column(String(64), nullable=True, index=True)
    import_external_id = Column(String(128), nullable=True, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="festivals")


class EventManagementEvent(Base):
    __tablename__ = "event_management_events"

    id = Column(Integer, primary_key=True)
    creator_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    festival_id = Column(Integer, ForeignKey("festivals.id", ondelete="SET NULL"), nullable=True, index=True)

    festival_name = Column(String(255), nullable=False, index=True)
    event_start_date = Column(Date, nullable=True, index=True)
    event_end_date = Column(Date, nullable=True, index=True)
    arrival_at = Column(DateTime(timezone=True), nullable=True)
    departure_at = Column(DateTime(timezone=True), nullable=True)
    address = Column(Text, nullable=True)
    leader_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    leader_name = Column(String(255), nullable=True)
    floor_plan_path = Column(String(255), nullable=True)
    venue_total_area_m2 = Column(Float, nullable=True)
    venue_main_stage_width_m = Column(Float, nullable=True)
    venue_main_stage_length_m = Column(Float, nullable=True)
    venue_small_stage_width_m = Column(Float, nullable=True)
    venue_small_stage_length_m = Column(Float, nullable=True)
    venue_sound_equipment_json = Column(JSON, nullable=False, default=list)
    venue_light_equipment_json = Column(JSON, nullable=False, default=list)
    venue_wardrobe = Column(Text, nullable=True)
    venue_dressing_rooms = Column(Text, nullable=True)
    venue_medical_point = Column(Text, nullable=True)
    venue_security = Column(Text, nullable=True)
    venue_admin_name = Column(String(255), nullable=True)
    venue_admin_contact = Column(String(255), nullable=True)

    team_rows_json = Column(JSON, nullable=False, default=list)
    halls_json = Column(JSON, nullable=False, default=list)
    stage_rows_json = Column(JSON, nullable=False, default=list)
    nomination_prize_rows_json = Column(JSON, nullable=False, default=list)
    accreditation_rows_json = Column(JSON, nullable=False, default=list)
    contractor_payment_rows_json = Column(JSON, nullable=False, default=list)
    ticket_rows_json = Column(JSON, nullable=False, default=list)
    ticket_promo_rows_json = Column(JSON, nullable=False, default=list)
    nomination_application_count = Column(Integer, nullable=True)
    announcements_json = Column(JSON, nullable=False, default=list)
    mail_template_rows_json = Column(JSON, nullable=False, default=list)
    promo_materials_json = Column(JSON, nullable=False, default=list)
    work_tasks_json = Column(JSON, nullable=False, default=list)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    creator = relationship("User", back_populates="event_management_events", foreign_keys=[creator_user_id])
    leader = relationship("User", foreign_keys=[leader_user_id])
    festival = relationship("Festival")


class ProjectSearchPost(Base):
    __tablename__ = "project_search_posts"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    fandom = Column(String(255), nullable=False, index=True)
    city = Column(String(255), nullable=True, index=True)
    event_date = Column(Date, nullable=True, index=True)
    event_type = Column(String(32), nullable=False, index=True)  # photoset | festival
    status = Column(String(32), nullable=False, default="active", index=True)  # active | found | inactive
    comment = Column(Text, nullable=True)
    contact_nick = Column(String(100), nullable=False)
    contact_link = Column(String(255), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="project_search_posts")
    comments = relationship("ProjectSearchComment", back_populates="post", cascade="all, delete-orphan")


class ProjectSearchComment(Base):
    __tablename__ = "project_search_comments"

    id = Column(Integer, primary_key=True)
    post_id = Column(Integer, ForeignKey("project_search_posts.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    post = relationship("ProjectSearchPost", back_populates="comments")
    user = relationship("User", back_populates="project_search_comments")


class FestivalNotification(Base):
    __tablename__ = "festival_notifications"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    from_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    source_card_id = Column(Integer, ForeignKey("cosplan_cards.id", ondelete="SET NULL"), nullable=True, index=True)
    reply_to_notification_id = Column(Integer, ForeignKey("festival_notifications.id", ondelete="SET NULL"), nullable=True, index=True)
    message = Column(Text, nullable=False)
    is_read = Column(Boolean, nullable=False, default=False)
    telegram_sent_at = Column(DateTime(timezone=True), nullable=True, index=True)
    vk_sent_at = Column(DateTime(timezone=True), nullable=True, index=True)
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


class PhotoContestRequest(Base):
    __tablename__ = "photo_contest_requests"

    id = Column(Integer, primary_key=True)
    requester_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    reviewed_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    contest_id = Column(Integer, ForeignKey("photo_contests.id", ondelete="SET NULL"), nullable=True, index=True)
    festival_id = Column(Integer, ForeignKey("festivals.id", ondelete="SET NULL"), nullable=True, index=True)

    title = Column(String(255), nullable=False, index=True)
    submission_start_date = Column(Date, nullable=True)
    submission_end_date = Column(Date, nullable=True, index=True)
    results_date = Column(Date, nullable=True, index=True)
    nominations_json = Column(JSON, nullable=False, default=list)
    festival_name = Column(String(255), nullable=True)
    judging_format = Column(String(32), nullable=False, default="open", index=True)  # open | closed
    judges_json = Column(JSON, nullable=False, default=list)
    rules_markdown = Column(Text, nullable=True)
    prizes_markdown = Column(Text, nullable=True)
    max_photos_per_participant = Column(Integer, nullable=False, default=1)
    participant_visibility = Column(String(32), nullable=False, default="all", index=True)  # all | winners
    status = Column(String(16), nullable=False, default="pending", index=True)  # pending | approved | rejected
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    requester = relationship("User", back_populates="photo_contest_requests", foreign_keys=[requester_user_id])
    reviewer = relationship("User", back_populates="photo_contest_requests_reviewed", foreign_keys=[reviewed_by_user_id])


class PhotoContest(Base):
    __tablename__ = "photo_contests"

    id = Column(Integer, primary_key=True)
    creator_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    approved_request_id = Column(Integer, ForeignKey("photo_contest_requests.id", ondelete="SET NULL"), nullable=True, unique=True, index=True)
    festival_id = Column(Integer, ForeignKey("festivals.id", ondelete="SET NULL"), nullable=True, index=True)

    title = Column(String(255), nullable=False, index=True)
    submission_start_date = Column(Date, nullable=True)
    submission_end_date = Column(Date, nullable=True, index=True)
    results_date = Column(Date, nullable=True, index=True)
    nominations_json = Column(JSON, nullable=False, default=list)
    festival_name = Column(String(255), nullable=True)
    judging_format = Column(String(32), nullable=False, default="open", index=True)  # open | closed
    judges_json = Column(JSON, nullable=False, default=list)
    rules_markdown = Column(Text, nullable=True)
    prizes_markdown = Column(Text, nullable=True)
    max_photos_per_participant = Column(Integer, nullable=False, default=1)
    participant_visibility = Column(String(32), nullable=False, default="all", index=True)  # all | winners
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    creator = relationship("User", back_populates="photo_contests", foreign_keys=[creator_user_id])
    entries = relationship("PhotoContestEntry", back_populates="contest", cascade="all, delete-orphan")
    photos = relationship("PhotoContestEntryPhoto", back_populates="contest", cascade="all, delete-orphan")
    votes = relationship("PhotoContestVote", back_populates="contest", cascade="all, delete-orphan")


class PhotoContestEntry(Base):
    __tablename__ = "photo_contest_entries"

    id = Column(Integer, primary_key=True)
    contest_id = Column(Integer, ForeignKey("photo_contests.id", ondelete="CASCADE"), nullable=False, index=True)
    participant_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    nomination_title = Column(String(255), nullable=True, index=True)
    fandom = Column(String(255), nullable=True)
    characters_json = Column(JSON, nullable=False, default=list)
    roles_json = Column(JSON, nullable=False, default=list)
    agreed_to_rules = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    contest = relationship("PhotoContest", back_populates="entries")
    participant = relationship("User", back_populates="photo_contest_entries")
    photos = relationship("PhotoContestEntryPhoto", back_populates="entry", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("contest_id", "participant_user_id", name="uq_photo_contest_entry_participant"),
    )


class PhotoContestEntryPhoto(Base):
    __tablename__ = "photo_contest_entry_photos"

    id = Column(Integer, primary_key=True)
    contest_id = Column(Integer, ForeignKey("photo_contests.id", ondelete="CASCADE"), nullable=False, index=True)
    entry_id = Column(Integer, ForeignKey("photo_contest_entries.id", ondelete="CASCADE"), nullable=False, index=True)
    file_path = Column(String(255), nullable=False)
    sort_order = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    contest = relationship("PhotoContest", back_populates="photos")
    entry = relationship("PhotoContestEntry", back_populates="photos")
    votes = relationship("PhotoContestVote", back_populates="photo", cascade="all, delete-orphan")


class PhotoContestVote(Base):
    __tablename__ = "photo_contest_votes"

    id = Column(Integer, primary_key=True)
    contest_id = Column(Integer, ForeignKey("photo_contests.id", ondelete="CASCADE"), nullable=False, index=True)
    photo_id = Column(Integer, ForeignKey("photo_contest_entry_photos.id", ondelete="CASCADE"), nullable=False, index=True)
    voter_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    contest = relationship("PhotoContest", back_populates="votes")
    photo = relationship("PhotoContestEntryPhoto", back_populates="votes")
    voter = relationship("User", back_populates="photo_contest_votes")

    __table_args__ = (
        UniqueConstraint("contest_id", "photo_id", "voter_user_id", name="uq_photo_contest_vote"),
    )


class CommunityArticle(Base):
    __tablename__ = "community_articles"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    topic = Column(String(255), nullable=False, index=True)
    author_name = Column(String(120), nullable=False)
    body_markdown = Column(Text, nullable=False)
    tags_json = Column(JSON, nullable=False, default=list)
    import_source = Column(String(64), nullable=True, index=True)
    import_external_id = Column(String(128), nullable=True, index=True)
    import_url = Column(Text, nullable=True)

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
    is_anonymous = Column(Boolean, nullable=False, default=False)
    topics_json = Column(JSON, nullable=False, default=list)

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
    city = Column(String(255), nullable=True, index=True)
    master_type = Column(String(64), nullable=False, index=True)
    details = Column(Text, nullable=False)
    gallery_json = Column(JSON, nullable=False, default=list)
    price_list_json = Column(JSON, nullable=False, default=list)
    allow_site_orders = Column(Boolean, nullable=False, default=False, server_default="0")
    queue_card_ids_json = Column(JSON, nullable=False, default=list)
    queue_show_deadline = Column(Boolean, nullable=False, default=True, server_default="1")
    queue_show_progress = Column(Boolean, nullable=False, default=True, server_default="1")
    import_source = Column(String(64), nullable=True, index=True)
    import_external_id = Column(String(128), nullable=True, index=True)
    import_url = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_masters")
    comments = relationship("CommunityMasterComment", back_populates="master", cascade="all, delete-orphan")
    orders = relationship("CommunityMasterOrder", back_populates="master", cascade="all, delete-orphan")
    ratings = relationship("CommunityMasterRating", back_populates="master", cascade="all, delete-orphan")


class CommunityMasterComment(Base):
    __tablename__ = "community_master_comments"

    id = Column(Integer, primary_key=True)
    master_id = Column(Integer, ForeignKey("community_masters.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    is_client = Column(Boolean, nullable=False, default=False)
    images_json = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    master = relationship("CommunityMaster", back_populates="comments")
    user = relationship("User", back_populates="community_master_comments")


class CommunityMasterRating(Base):
    __tablename__ = "community_master_ratings"

    id = Column(Integer, primary_key=True)
    master_id = Column(Integer, ForeignKey("community_masters.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    stars = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    master = relationship("CommunityMaster", back_populates="ratings")
    user = relationship("User", back_populates="community_master_ratings")

    __table_args__ = (
        UniqueConstraint("master_id", "user_id", name="uq_community_master_rating_user"),
    )


class CommunityMasterOrder(Base):
    __tablename__ = "community_master_orders"

    id = Column(Integer, primary_key=True)
    master_id = Column(Integer, ForeignKey("community_masters.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    subject = Column(String(255), nullable=False)
    contact_tg = Column(String(255), nullable=True)
    character_fandom = Column(String(255), nullable=True)
    details = Column(Text, nullable=True)
    deadline = Column(Date, nullable=True, index=True)
    references_json = Column(JSON, nullable=False, default=list)
    status = Column(String(32), nullable=False, default="pending", index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    master = relationship("CommunityMaster", back_populates="orders")
    customer = relationship("User", back_populates="community_master_orders")


class CommunityMasterSearchPost(Base):
    __tablename__ = "community_master_search_posts"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    subject = Column(String(255), nullable=False, index=True)
    master_type = Column(String(64), nullable=False, index=True)
    contact_tg = Column(String(255), nullable=True)
    character_fandom = Column(String(255), nullable=True)
    details = Column(Text, nullable=True)
    deadline = Column(Date, nullable=True, index=True)
    references_json = Column(JSON, nullable=False, default=list)
    budget_rub = Column(Integer, nullable=True)
    is_price_negotiable = Column(Boolean, nullable=False, default=False, server_default="0")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_master_search_posts")
    comments = relationship("CommunityMasterSearchComment", back_populates="post", cascade="all, delete-orphan")


class CommunityMasterSearchComment(Base):
    __tablename__ = "community_master_search_comments"

    id = Column(Integer, primary_key=True)
    post_id = Column(Integer, ForeignKey("community_master_search_posts.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    post = relationship("CommunityMasterSearchPost", back_populates="comments")
    user = relationship("User", back_populates="community_master_search_comments")


class CommunityStudio(Base):
    __tablename__ = "community_studios"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    name = Column(String(255), nullable=False, index=True)
    city = Column(String(255), nullable=False, index=True)
    address = Column(String(255), nullable=True)
    gallery_json = Column(JSON, nullable=False, default=list)
    contact = Column(String(255), nullable=True)
    note = Column(Text, nullable=True)
    price_list_json = Column(JSON, nullable=False, default=list)
    tags_json = Column(JSON, nullable=False, default=list)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_studios")
    comments = relationship("CommunityStudioComment", back_populates="studio", cascade="all, delete-orphan")


class CommunityStudioComment(Base):
    __tablename__ = "community_studio_comments"

    id = Column(Integer, primary_key=True)
    studio_id = Column(Integer, ForeignKey("community_studios.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    studio = relationship("CommunityStudio", back_populates="comments")
    user = relationship("User", back_populates="community_studio_comments")


class CommunityMarketplaceSale(Base):
    __tablename__ = "community_marketplace_sales"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    name = Column(String(255), nullable=False, index=True)
    city = Column(String(255), nullable=True, index=True)
    contact = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    gallery_json = Column(JSON, nullable=False, default=list)
    price_list_json = Column(JSON, nullable=False, default=list)
    delivery_terms = Column(Text, nullable=True)
    is_verified_participant = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_marketplace_sales")


class CommunityMarketplaceSearch(Base):
    __tablename__ = "community_marketplace_searches"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    name = Column(String(255), nullable=False, index=True)
    city = Column(String(255), nullable=True, index=True)
    description = Column(Text, nullable=True)
    references_json = Column(JSON, nullable=False, default=list)
    budget = Column(String(120), nullable=True)
    is_verified_participant = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_marketplace_searches")


class CommunityCosplayer(Base):
    __tablename__ = "community_cosplayers"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    nick = Column(String(100), nullable=False, index=True)
    tg_channel = Column(String(255), nullable=True)
    city = Column(String(255), nullable=True, index=True)
    favorite_directions = Column(Text, nullable=True)
    promo_photos_json = Column(JSON, nullable=False, default=list)
    about_markdown = Column(Text, nullable=True)
    collab_status = Column(String(32), nullable=False, default="open", index=True)
    extra_skills_json = Column(JSON, nullable=False, default=list)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_cosplayers")
    comments = relationship("CommunityCosplayerComment", back_populates="cosplayer", cascade="all, delete-orphan")


class CommunityCosplayerComment(Base):
    __tablename__ = "community_cosplayer_comments"

    id = Column(Integer, primary_key=True)
    cosplayer_id = Column(Integer, ForeignKey("community_cosplayers.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    cosplayer = relationship("CommunityCosplayer", back_populates="comments")
    user = relationship("User", back_populates="community_cosplayer_comments")


class CommunityMaterialPlace(Base):
    __tablename__ = "community_material_places"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    name = Column(String(255), nullable=False, index=True)
    types_json = Column(JSON, nullable=False, default=list)
    city = Column(String(255), nullable=True, index=True)
    price_level = Column(Integer, nullable=True, index=True)
    address = Column(String(255), nullable=True)
    link = Column(Text, nullable=True)
    work_hours_json = Column(JSON, nullable=False, default=list)
    description = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="community_material_places")
    comments = relationship("CommunityMaterialPlaceComment", back_populates="place", cascade="all, delete-orphan")


class CommunityMaterialPlaceComment(Base):
    __tablename__ = "community_material_place_comments"

    id = Column(Integer, primary_key=True)
    place_id = Column(Integer, ForeignKey("community_material_places.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    place = relationship("CommunityMaterialPlace", back_populates="comments")
    user = relationship("User", back_populates="community_material_place_comments")
