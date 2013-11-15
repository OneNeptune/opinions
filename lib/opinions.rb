require 'opinions/version'
require 'singleton'
require 'redis'

module Opinions

  class << self
    attr_accessor :backend
  end

  module KeyBuilderExtensions

    def generate_key(scope, id = nil)
      [self.class.name, scope, id].compact.join(':')
    end

  end

  class KeyBuilder

    def initialize(args)
      @object  = args.fetch(:object)
      @target  = args.fetch(:target, nil)
      @opinion = args.fetch(:opinion)
    end

    def key
      object_id = @object.id
      object = @object.dup
      object.class.send(:include, KeyBuilderExtensions)
      key = object.generate_key(@opinion, object_id)
      if @target
        tcn = @target.class == Class ? @target.name : @target.class.name
        key += ":#{tcn}"
      end
      key
    end

  end

  class OpinionFactory
    attr_reader :key_name
    def initialize(args)
      @direction = args.has_key?(:from_target) ? :ot : :to
      @key_name  = args.first[1]
    end
    def opinion
      Opinions.backend.read_key(key_name).collect do |object_id, time|
        target_class_name, opinion, target_id, object_class_name = key_name.split ':'
        target_class, object_class = Kernel.const_get(target_class_name), Kernel.const_get(object_class_name)
        begin 
          object_instance = object_class.find(object_id)
          target_instance = target_class.find(target_id)
          Opinion.new(target: (@direction == :to ? object_instance : target_instance),
                      object: (@direction == :to ? target_instance : object_instance),
                      opinion: opinion.to_sym,
                      created_at: time)
        rescue
          nil
        end   
      end
    end
  end

  class RedisBackend

    attr_accessor :redis

    def write_keys(key_hashes)
      redis.multi do
        key_hashes.each do |key_name, hash|
          write_key(key_name, hash)
        end
      end
    end

    def write_key(key_name, hash)
      hash.each do |hash_key, hash_value|
        redis.hset key_name, hash_key, hash_value
      end
    end
    private :write_key

    def read_key(key_name)
      redis.hgetall(key_name)
    end

    def read_sub_key(key_name, key)
      redis.hget(key_name, key)
    end

    def remove_keys(keys)
      redis.multi do
        keys.each do |key_name|
          redis.del(key_name)
        end
      end
    end

    def remove_sub_keys(key_pairs)
      redis.multi do
        key_pairs.each do |key_name, key|
          redis.hdel(key_name, key.to_s)
        end
      end
    end

    def keys_matching(argument)
      redis.keys(argument)
    end

  end

  class KeyLoader

    def initialize(key)
      @object_class, @opinion, @object_id, @target_class = key.split(':')
    end

    def object
      Object.const_get(@object_class).find(_object_id)
    end

    private

      def _object_id
        @object_id.to_i == @object_id ? @object_id : @object_id.to_i
      end

  end

  class OpinionRemover

    def self.remove(target)
      false unless target.class.instance_variable_defined? :@registered_opinions      
      (target.class.instance_variable_get :@registered_opinions).each do |opinion|
        lookup_key_builder = KeyBuilder.new(object: target, opinion: opinion)
        keys = Opinions.backend.keys_matching(lookup_key_builder.key + "*")
        keys.collect do |key_name|
          Opinions.backend.read_key(key_name).collect do |object_id, time|
            target_class_name, opinion, target_id, object_class_name = key_name.split ':'
            opposite_key_name = [object_class_name, opinion, object_id, target_class_name].compact.join(':')
            Opinions.backend.remove_sub_keys([[opposite_key_name, target_id]])
          end
          Opinions.backend.remove_keys([key_name])
        end
      end
    end
  end

  class Opinion

    attr_accessor :target, :object, :opinion, :created_at

    def initialize(args = {})
      @target, @object, @opinion, @created_at =
        args.fetch(:target), args.fetch(:object), args.fetch(:opinion), args.fetch(:created_at, nil)
      self
    end

    def persist(args = {time: Time.now.utc})
      backend.write_keys({
        target_key => {object.id.to_s => args.fetch(:time)},
        object_key => {target.id.to_s => args.fetch(:time)},
      })
    end

    def object_key
      KeyBuilder.new(object: object, opinion: opinion, target: target).key
    end

    def target_key
      KeyBuilder.new(object: target, opinion: opinion, target: object).key
    end

    def exists?
      tk = backend.read_sub_key(target_key, object.id.to_s)
      ok = backend.read_sub_key(object_key, target.id.to_s)
      tk && ok
    end

    def remove
      backend.remove_sub_keys([[target_key, object.id.to_s],
                               [object_key, target.id.to_s]])
    end

    def ==(other_opinion)
      raise ArgumentError, "Can't compare a #{other_opinion} with #{self}" unless other_opinion.is_a?(Opinion)
      opinion_equal  = self.opinion == other_opinion.opinion
      opinion_target = self.target  == other_opinion.target
      opinion_object = self.object  == other_opinion.object
      opinion_equal && opinion_target && opinion_object
    end

    private

      def backend
        Opinions.backend
      end

  end

  module Pollable

    class << self

      def included(klass)
        klass.send(:extend,  ClassMethods)
      end

    end

    module ClassMethods

      def opinions(*opinions)
        opinions.each { |opinion| register_opinion(opinion.to_sym) }
      end

      def registered_opinions
        @registered_opinions
      end

      def register_opinion(opinion)
        @registered_opinions ||= Array.new
        @registered_opinions <<  opinion
        
        self.send :define_method, :"#{opinion}_by" do |*args|
          opinionated, time = *args
          time              = time || Time.now.utc
          e = Opinion.new(object: opinionated, target: self, opinion: opinion)
          true & e.persist(time: time)
        end

        self.send :define_method, :"cancel_#{opinion}_by" do |opinionated|
          true & Opinion.new(object: opinionated, target: self, opinion: opinion).remove
        end

        self.send :define_method, :"#{opinion}_votes" do
          lookup_key_builder = KeyBuilder.new(object: self, opinion: opinion)
          keys = Opinions.backend.keys_matching(lookup_key_builder.key + "*")
          keys.collect do |key_name|
            OpinionFactory.new(from_target: key_name).opinion
          end.flatten.compact
        end
        
        self.send :define_method, :"remove_votes" do
          true & OpinionRemover.remove( self )
        end
      end
    end
  end

  module Opinionated

    def self.included(klass)
      klass.send(:extend,  ClassMethods)
    end

    module ClassMethods

      def opinions(*opinions)
        opinions.each { |opinion| register_opinion(opinion.to_sym) }
      end

      def registered_opinions
        @registered_opinions
      end

      def register_opinion(opinion)
        @registered_opinions ||= Array.new
        @registered_opinions <<  opinion

        self.send :define_method, :"#{opinion}" do |*args|
          target, time = *args
          time         = time || Time.now.utc
          opinion_instance = Opinion.new(object: self, target: target, opinion: opinion)
          backend_opinion = opinion_instance.persist(time: time)
          opinion_instance if backend_opinion.present?
        end

        self.send :define_method, :"cancel_#{opinion}" do |pollable|
          opinion_instance = Opinion.new(object: self, target: pollable, opinion: opinion)
          backend_opinion = opinion_instance.remove
          opinion_instance if backend_opinion.present?
        end

        self.send :define_method, :"have_#{opinion}_on" do |pollable|
          send("#{opinion}_opinions").collect { |o| o.target == pollable }.any?
        end

        self.send :define_method, :"#{opinion}_opinions" do |pollable = nil|
          lookup_key_builder = KeyBuilder.new(object: self, opinion: opinion, target: pollable)
          keys = Opinions.backend.keys_matching(lookup_key_builder.key + "*")
          keys.collect do |key_name|
            OpinionFactory.new(from_object: key_name).opinion
          end.flatten.compact
        end

        self.send :define_method, :"remove_opinions" do
          true & OpinionRemover.remove(self)
        end
      end

    end
  end

end
