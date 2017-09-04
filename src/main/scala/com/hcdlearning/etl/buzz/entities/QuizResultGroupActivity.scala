package com.hcdlearning.etl.buzz.entities

case class QuizResultGroupActivity(archive_date: String,
                                   archive_time: String,
                                   activity: String,
                                   member_id: String,
                                   lesson_id: String,
                                   `type`: String,
                                   quiz_result_group_id: String,
                                   correct: Int,
                                   wrong: Int,
                                   total: Int,
                                   start_date: String)
