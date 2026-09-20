---
title: "Railsのレイヤードアーキテクチャを運用する: Layered Designを補強する実践"
emoji: "💎"
type: "tech" # tech: 技術記事 / idea: アイデア
topics: ["rails", "ruby", "design", "設計"]
published: true
---

少し前にVladimir Dementyev著『Layered Design for Ruby on Rails Applications』を読みました。

私は普段、10年以上運用されている大規模なRailsアプリケーションに新しい機能を追加しています。既存のコードへ新しい責務を追加するヒントを得るために読みました。英語のリーディング練習も兼ねています。翻訳や内容の整理には、かなりAIも頼っています。

この本では、RailsのMVCを捨てるのではなく、その上にPresentation、Application、Domain、Infrastructureの層を重ねる考え方を**The Extended Rails Way**と呼んでいます。

私はこれを、Railsの開発速度を保ちながら、成長したコードの責務を整理するための考え方として読んでいます。

https://link.amazon/B01I0w9fF

#### 1. 概要
本書は、Rails標準のMVCを破棄するのではなく、その上に必要な階層（Presentation / Application / Domain / Infrastructure）を重ねるアプローチを提案しています。

##### 依存関係の鉄則
この記事では、ソースコード上の依存をPresentation → Application → Domain → Infrastructureの順に限定します。`params`やセッションなどHTTP固有の情報はPresentation層で受け取ります。アプリケーション層やドメイン層へは、ユースケースに必要な引数へ変換して渡します。したがって、アプリケーションサービスやドメインモデルから`params`、セッション、`current_user`を直接参照することは避けます。

層を増やすこと自体は目的ではありません。データを素通りさせるだけの層を作らず、各層に明確な責務を持たせます。

##### モデルとコントローラーを解体する抽象化パターン
ここでは、肥大化したモデルやコントローラーから切り出す候補を整理します。すべてを導入する必要はありません。変更理由が異なる処理を見つけたときに、どの抽象化が自然かを考えるために使います。

| 層             | パターン                               | 主な責務                                       | 導入を検討する場面                                  |
| -------------- | -------------------------------------- | ---------------------------------------------- | --------------------------------------------------- |
| Presentation   | Form Object                            | フォーム入力の変換と検証                       | 1つの画面で複数モデルを更新する、UI固有の検証がある |
| Presentation   | Filter Object                          | パラメータから検索条件を組み立てる             | 一覧の絞り込み・並び替えが増えた                    |
| Presentation   | Serializer / Presenter / ViewComponent | 表示・APIレスポンスへの変換                    | モデルの属性をそのまま外部へ出したくない            |
| Application    | Service Object                         | ユースケースの手順とトランザクションを調整する | 複数のドメイン操作や外部連携を順に実行する          |
| Application    | Notification Layer                     | 通知の要求と配信チャネルを分ける               | メール以外の通知手段や配信条件が増えた              |
| Domain         | ActiveRecord Model / Value Object      | 状態、不変条件、計算規則を表す                 | どの入口からも守るべき業務ルールがある              |
| Domain         | Query Object                           | 読み取り用の複雑なクエリを表す                 | scopeが増え、SQLの意図を名前で表したい              |
| Domain         | Policy Object                          | 認可規則を一箇所に集める                       | コントローラーやビューに認可条件が散っている        |
| Domain         | Repository Pattern                     | 永続化をドメイン固有の操作として表す           | モジュール境界を越えるDBアクセスを限定したい        |
| Infrastructure | Configuration Object                   | ENVやCredentialsの読み込みを集約する           | 環境依存の分岐がアプリケーション内に散っている      |
| Infrastructure | Adapter                                | 外部APIやSDKとの変換を隔離する                 | 特定ベンダーの型や例外が上位層へ漏れている          |

この表は「どのクラスを作るか」を決めるためのものではありません。変更理由を一箇所へ集め、依存の向きを守れる最小の抽象化を選びます。

#### 2. 所感

RailsでMVC以外の設計を扱った情報は、探してみるとなかなか見つかりません。Railsの開発ではMVCが広く知られているからこそ、MVCの上にどのような層やオブジェクトを重ねていくのかを体系的に扱った本書は、貴重な資料だと感じました。

特に参考になったのは、**「ドメインのルール（不変条件）」と「アプリケーションの手順（ユースケース）」を分けて考える視点**です。

例えば、「注文は与信枠を超えられない」というルールは、どの入口から実行されても守るべきドメインの責務です。一方、「注文を作成し、決済を依頼し、通知を予約する」はユースケースの手順であり、アプリケーションサービスが調整します。

この区別を意識すると、モデルへ置くべきルールと、サービスで調整すべき処理を考えやすくなります。大規模なRailsアプリケーションへ新しい機能を追加するときにも、既存のモデルやコントローラーへ処理を足す前に、まずその処理の性質を見極めるための手がかりになります。

#### 3. 実践・ガードレール

ここからは、私の経験から加えられそうなアレンジを紹介します。レイヤーの境界を決めるだけでなく、依存方向や隠れた副作用を継続的に確認するためのガードレールです。

##### 依存の向きはLayer Checkerで検証する
依存方向のルールをドキュメントやレビューだけで守り続けるのは難しいため、私は[Packwerk ExtensionsのLayer Checker](https://github.com/rubyatscale/packwerk-extensions#layer-checker)による自動検証を候補にします。Packwerkのパッケージ境界を定義したうえで、各パッケージへ層を割り当てます。

```ruby
# Gemfile
gem "packwerk"
gem "packwerk-extensions"
```

```yaml
# packwerk.yml
require:
  - packwerk-extensions

layers:
  - presentation
  - application
  - domain
  - infrastructure
```

```yaml
# packages/orders/package.yml
enforce_layers: true
layer: application
```

この設定では、各パッケージは同じ層か下位層にだけ依存できます。例えば application のパッケージは domain や infrastructure に依存できますが、presentation には依存できません。

```bash
bundle exec packwerk check
```

このコマンドをCIで実行すると、パッケージをまたぐ逆方向の依存を検出できます。これはこの記事で採る直接依存のルールを検証する設定であり、依存性逆転を採る設計にそのまま当てはめるものではありません。

##### モデルコールバックから副作用を引き剥がす
レイヤーを分けても、モデルのコールバックに外部通知や別モデルの更新が隠れていると、保存処理が実際に何をするのか分かりにくいままです。特にConcernや親クラスから登録されたコールバックは、モデルファイルを読むだけでは把握しにくくなります。

いくつか参考になりそうな分類を考えました。

| 種類               | 例                            | 置き場所の目安                  |
| ------------------ | ----------------------------- | ------------------------------- |
| 不変条件           | 保存前の値の整合性を検証する  | Domain                          |
| ユースケースの手順 | 別モデルの作成、通知の予約    | Application                     |
| 外部への副作用     | メール、外部API、ジョブの実行 | ApplicationまたはInfrastructure |

すべてのコールバックをなくす必要はありません。どの入口から保存しても守るべき不変条件はモデルに残せます。一方、特定の画面やユースケースでだけ必要な処理は、Serviceなどから明示的に呼び出す方が、実行順序と失敗時の振る舞いを確認しやすくなります。

コールバックの棚卸しには、モデルに実際に登録された情報を一覧化するツールも使えます。手前味噌ですが、私が作った[annotate_callbacks](https://github.com/sloppybook/annotate_callbacks)というGemもこの用途に使えます。Concernや親クラス由来のコールバックを調べ、モデルファイルにコメントとして表示するツールです。可視化した一覧を起点に、残すコールバックと移動する副作用を一つずつ判断します。

##### 遅延評価によるクエリの漏れ出しを防ぐ：strict_loading
レイヤードアーキテクチャを導入しても、Active Recordの遅延評価（Lazy Loading）によって Presentation層（ViewやSerializer）で突然SQLが発行され、N+1問題や層の境界突破が発生することがあります。

これを防ぐために、Railsの `strict_loading` をガードレールとして有効化するのが効果的です。

```ruby
# config/application.rb
# アプリケーション全体で遅延ロードを禁止（必要な関連はQuery Object等でプリロードを強制）
config.active_record.strict_loading_by_default = true
config.active_record.action_on_strict_loading_violation = :raise
```

既存アプリケーション全体へ一度に適用するのが難しい場合は、モデル単位でも設定できます。

```ruby
class Post < ApplicationRecord
  self.strict_loading_by_default = true
end
```

さらに、Query Objectが返すRelationに限定して適用できます。

```ruby
class PublishedPostsQuery
  def self.call
    Post.where(status: :published)
        .strict_loading
        .includes(:author)
  end
end
```

Query ObjectやRepositoryで必要な関連（`.includes`や`.preload`）を明示的に取得していない場合、View側で関連にアクセスした時点でエラーが発生します。

これにより、「Presentation層で予期せぬクエリが評価・発行される」という抽象化の破綻を検知できます。

アプリケーション全体、モデル単位、クエリ単位のどこまで適用するかは、既存コードの量と移行コストに応じて決められます。

##### 段階的なリファクタリング（Gradual Refactoring）の進め方
本書は、最初からすべてを階層化するのではなく、変更頻度（Churn）と複雑度（Complexity）が高い箇所から段階的に切り出す進め方を推奨しています。変更されにくい箇所を先に抽象化すると、かえって不要な構造を増やしかねないためです。

この判断には、[RubyCritic](https://github.com/whitesmith/rubycritic)が使えそうだと感じました。RubyCriticは複雑度、重複、コードスメルをレポートするGemです。

```ruby
# Gemfile
gem "rubycritic", require: false
```

```bash
bundle exec rubycritic app/models app/controllers app/services
```
