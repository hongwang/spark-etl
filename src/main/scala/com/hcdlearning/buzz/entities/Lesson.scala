package com.hcdlearning.buzz.entities

case class Lesson(category: String,
                 level: String,
                 lesson_id: String,
                 date: String,
                 enabled: Boolean,
                 new_words_path: String,
                 quiz_path: String,
                 video_path: String)
