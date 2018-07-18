/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

/*
class Footer extends React.Component {
  docUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + 'docs/' + (language ? language + '/' : '') + doc;
  }

  pageUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + (language ? language + '/' : '') + doc;
  }

  render() {
    const currentYear = new Date().getFullYear();
    return (
      <footer className="nav-footer" id="footer">
        <section className="sitemap">
          <a href={this.props.config.baseUrl} className="nav-home">
            {this.props.config.footerIcon && (
              <img
                src={this.props.config.baseUrl + this.props.config.footerIcon}
                alt={this.props.config.title}
                width="66"
                height="58"
              />
            )}
          </a>
          <div>
            <h5>Docs</h5>
            <a href={this.docUrl('doc1.html', this.props.language)}>
              Getting Started (or other categories)
            </a>
            <a href={this.docUrl('doc2.html', this.props.language)}>
              Guides (or other categories)
            </a>
            <a href={this.docUrl('doc3.html', this.props.language)}>
              API Reference (or other categories)
            </a>
          </div>
          <div>
            <h5>Community</h5>
            <a href={this.pageUrl('users.html', this.props.language)}>
              User Showcase
            </a>
            <a
              href="http://stackoverflow.com/questions/tagged/"
              target="_blank"
              rel="noreferrer noopener">
              Stack Overflow
            </a>
            <a href="https://discordapp.com/">Project Chat</a>
            <a
              href="https://twitter.com/"
              target="_blank"
              rel="noreferrer noopener">
              Twitter
            </a>
          </div>
          <div>
            <h5>More</h5>
            <a href={this.props.config.baseUrl + 'blog'}>Blog</a>
            <a href="https://github.com/">GitHub</a>
            <a
              className="github-button"
              href={this.props.config.repoUrl}
              data-icon="octicon-star"
              data-count-href="/facebook/docusaurus/stargazers"
              data-show-count={true}
              data-count-aria-label="# stargazers on GitHub"
              aria-label="Star this project on GitHub">
              Star
            </a>
          </div>
        </section>

        <a
          href="https://code.facebook.com/projects/"
          target="_blank"
          rel="noreferrer noopener"
          className="fbOpenSource">
          <img
            src={this.props.config.baseUrl + 'img/oss_logo.png'}
            alt="Facebook Open Source"
            width="170"
            height="45"
          />
        </a>
        <section className="copyright">{this.props.config.copyright}</section>
      </footer>
    );
  }
}
*/

class Footer extends React.Component {
  docUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + 'docs/' + (language ? language + '/' : '') + doc;
  }

  pageUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + (language ? language + '/' : '') + doc;
  }

  render() {
    const currentYear = new Date().getFullYear();

    const contactUrl = this.pageUrl('contact')
    const eventsUrl = this.pageUrl('events')
    const twitterUrl = 'https://twitter.com/Apache_Pulsar'
    const wikiUrl = 'https://github.com/apache/incubator-pulsar/wiki'
    const issuesUrl = 'https://github.com/apache/incubator-pulsar/issues'
    const resourcesUrl = this.pageUrl('resources')
    const teamUrl = this.pageUrl('team')

    const communityMenuJs = `
      const community = document.querySelector("a[href='#community']").parentNode;
      const communityMenu = 
        '<li>' +
        '<a id="community-menu" href="#">Community</a>' + 
        '<div id="community-dropdown" class="hide">' +
          '<ul id="community-dropdown-items">' +
            '<li><a href="${contactUrl}">Contant</a></li>' +
            '<li><a href="${eventsUrl}">Events</a></li>' +
            '<li><a href="${twitterUrl}">Twitter</a></li>' +
            '<li><a href="${wikiUrl}">Wiki</a></li>' +
            '<li><a href="${issuesUrl}">Issue tracking</a></li>' +
            '<li><a href="${resourcesUrl}">Resources</a></li>' +
            '<li><a href="${teamUrl}">Team</a></li>' +
          '</ul>' +
        '</div>' +
        '</li>';

      community.innerHTML = communityMenu;

      const communityMenuItem = document.getElementById("community-menu");
      const communityDropDown = document.getElementById("community-dropdown");
      communityMenuItem.addEventListener("click", function(event) {
        event.preventDefault();

        if (communityDropDown.className == 'hide') {
          communityDropDown.className = 'visible';
        } else {
          communityDropDown.className = 'hide';
        }
      });
    `

    return (
      <footer className="nav-footer" id="footer">
        <section className="copyright">{this.props.config.copyright}</section>
        <span>
        <script dangerouslySetInnerHTML={{__html: communityMenuJs }}></script>
        </span>
        <span>
        <script src={this.props.config.baseUrl + 'js/pjax-api.min.js'}></script>
        <script dangerouslySetInnerHTML={{__html: `window.navfoo = new Pjax({
            areas: [
              // try to use the first query.
              '.mainContainer, .docsNavContainer .toc .navWrapper, .onPageNav',
              // fallback
              'body'
            ],
            link: '.docsNavContainer:not(.docsSliderActive) a',
            update: {
              script: false,
            }
          });
        `}}></script>
        </span>
      </footer>
    );
  }
}


module.exports = Footer;
